import assert from 'node:assert/strict';
import * as publicCore from '@smoothbricks/statebus-core';
import {
  type BusApiExports,
  bindEffect,
  createBusApi as createCoreBusApi,
  defineCapability,
  defineEffect,
  ManualScheduler,
} from '@smoothbricks/statebus-core';
import { createBusApi, type RequiredBindings } from '@smoothbricks/statebus-react';
import { Window } from 'happy-dom';
import { act, createElement as h, StrictMode } from 'react';

// Synthetic host/CMS shape: one public CMS API contains features; its host supplies the exact auth.
let authInitializations = 0;
const auth = createCoreBusApi({
  name: 'auth',
  setup(scope) {
    const valid = scope.scalar('valid', () => {
      authInitializations++;
      return false;
    });
    const granted = scope.event<null>('granted');
    const expired = scope.event<null>('expired');
    scope.reduce(granted, (state) => state.set(valid, true));
    scope.reduce(expired, (state) => state.set(valid, false));
    return { valid, granted, expired };
  },
});
type Auth = BusApiExports<typeof auth>;
const cmsAuth = defineCapability<Auth | null>('cms.auth');
const editorAuth = defineCapability<Auth | null>('cms.editor.auth');

const editor = createBusApi({
  name: 'editor',
  requires: [editorAuth],
  // Standalone policy: no session, so protected saves are refused.
  bindings: [editorAuth.provide(null)],
  setup(scope) {
    const access = scope.require(editorAuth);
    const saves = scope.scalar('saves', () => 0);
    const save = scope.command('save', (state, _requestId: number) => access !== null && state.read(access.valid));
    const saved = scope.event<number>('saved');
    scope.reduce(saved, (state) => state.set(saves, state.read(saves) + 1));
    const effect = defineEffect({
      command: save,
      result: saved,
      // Planning reads batch-final state, so an expiry later in the same wave still refuses execution.
      plan: (state, requestId) => (access !== null && state.read(access.valid) ? { requestId } : undefined),
      decode: (_plan, outcome: number) => outcome,
    });
    return { access, saves, save, effect };
  },
});
const content = createBusApi({ name: 'content', setup: (scope) => ({ title: scope.scalar('title', () => '') }) });
const media = createBusApi({
  name: 'media',
  requires: [cmsAuth],
  // Declaring the parent's token does not inherit the parent's value.
  bindings: [cmsAuth.provide(null)],
  setup: (scope) => ({ access: scope.require(cmsAuth) }),
});
let editorForwards = 0;
const cms = createBusApi({
  name: 'cms',
  requires: [cmsAuth],
  bindings: [cmsAuth.provide(null)],
  libraries: { content, media, editor },
  libraryBindings: {
    editor: (_libraries, parent) => {
      assert.equal('seal' in parent, false, 'Forwarding cannot seal a parent or sibling setup.');
      editorForwards++;
      return [editorAuth.provide(parent.require(cmsAuth))];
    },
  },
  setup: (scope) => ({ access: scope.require(cmsAuth) }),
});
let hostForwards = 0;
const host = createBusApi({
  name: 'host',
  libraries: { auth, cms },
  libraryBindings: {
    cms: (libraries) => {
      hostForwards++;
      return [cmsAuth.provide(libraries.get('auth'))];
    },
  },
  setup: () => ({}),
});
const portal = createBusApi({
  name: 'portal',
  libraries: { staffAuth: auth, partnerAuth: auth, staff: cms, partner: cms },
  libraryBindings: {
    staff: (libraries) => [cmsAuth.provide(libraries.get('staffAuth'))],
    partner: (libraries) => [cmsAuth.provide(libraries.get('partnerAuth'))],
  },
  setup: () => ({}),
});

let passed = 0;
async function test(name: string, run: () => void | Promise<void>): Promise<void> {
  await run();
  passed++;
  console.log(`PASS parent bindings: ${name}`);
}

await test('host -> CMS -> feature forwards the exact hosted auth, never the standalone default', async () => {
  // cms, host and portal (twice) each resolved one editor occurrence while being declared.
  assert.equal(Object.hasOwn(publicCore, 'mountResolvedLibrary'), false);
  assert.equal(Object.hasOwn(publicCore, 'RequiredBindings'), false);
  assert.equal(editorForwards, 4);
  assert.equal(hostForwards, 1);
  assert.equal(authInitializations, 0);
  const bus = host.createBus({ scheduler: new ManualScheduler() });
  try {
    const root = host.getBus(bus);
    const session = root.library('auth').exports;
    const hosted = root.library('cms');
    const model = hosted.library('editor').exports;
    assert.equal(hosted.exports.access, session);
    assert.equal(model.access, session);
    assert.equal(editor.getBus(bus).exports, model);
    assert.equal(hosted.library('media').exports.access, null, 'Forwarding is explicit, not ambient inheritance.');
    let calls = 0;
    bindEffect(bus, model.effect, { execute: () => ++calls, failure: () => -1 });
    bus.publish(model.save, 1);
    await bus.drain();
    assert.equal(calls, 0);
    bus.publish(session.granted, null);
    bus.publish(model.save, 2);
    await bus.drain();
    assert.equal(calls, 1);
    assert.equal(bus.read(model.saves), 1);
  } finally {
    bus.dispose();
  }
});

await test('standalone CMS and editor keep their explicit standalone defaults', async () => {
  for (const api of [cms, editor]) {
    const bus = api.createBus({ scheduler: new ManualScheduler() });
    try {
      const model = editor.getBus(bus).exports;
      assert.equal(model.access, null);
      let calls = 0;
      bindEffect(bus, model.effect, { execute: () => ++calls, failure: () => -1 });
      bus.publish(model.save, 1);
      await bus.drain();
      assert.equal(calls, 0);
    } finally {
      bus.dispose();
    }
  }
  const standalone = cms.createBus({ scheduler: new ManualScheduler() });
  const hosted = host.createBus({ scheduler: new ManualScheduler() });
  try {
    assert.notEqual(editor.getBus(standalone).exports, editor.getBus(hosted).exports);
    assert.equal(cms.getBus(standalone).exports.access, null);
  } finally {
    standalone.dispose();
    hosted.dispose();
  }
});

await test('two CMS occurrences forward their own different auth capabilities', async () => {
  const bus = portal.createBus({ scheduler: new ManualScheduler() });
  try {
    const root = portal.getBus(bus);
    const staffAuth = root.library('staffAuth').exports;
    const partnerAuth = root.library('partnerAuth').exports;
    const staff = root.library('staff').library('editor').exports;
    const partner = root.library('partner').library('editor').exports;
    assert.equal(staff.access, staffAuth);
    assert.equal(partner.access, partnerAuth);
    assert.notEqual(staffAuth, partnerAuth);
    assert.throws(() => editor.getBus(bus), /Ambiguous/);
    let staffCalls = 0;
    let partnerCalls = 0;
    bindEffect(bus, staff.effect, { execute: () => ++staffCalls, failure: () => -1 });
    bindEffect(bus, partner.effect, { execute: () => ++partnerCalls, failure: () => -1 });
    bus.publish(staffAuth.granted, null);
    bus.publish(staff.save, 1);
    bus.publish(partner.save, 1);
    await bus.drain();
    assert.equal(staffCalls, 1);
    assert.equal(partnerCalls, 0);
    assert.equal(bus.read(partnerAuth.valid), false);
  } finally {
    bus.dispose();
  }
});

await test('duplicate, missing, incompatible and undeclared parent bindings refuse before child resolution', () => {
  const forwards = editorForwards;
  const initializations = authInitializations;
  const foreign = defineCapability<Auth | null>('foreign.auth');
  assert.throws(
    () =>
      createCoreBusApi({
        name: 'missing-host',
        libraries: { auth, cms },
        libraryBindings: { cms: () => [] },
        setup: () => ({}),
      }),
    /Missing or incompatible required binding: cms.auth/,
  );
  assert.throws(
    () =>
      createCoreBusApi({
        name: 'duplicate-host',
        libraries: { auth, cms },
        libraryBindings: { cms: (libraries) => [cmsAuth.provide(libraries.get('auth')), cmsAuth.provide(null)] },
        setup: () => ({}),
      }),
    /Duplicate/,
  );
  assert.throws(
    () =>
      createCoreBusApi({
        name: 'foreign-host',
        libraries: { auth, cms },
        libraryBindings: { cms: (libraries) => [foreign.provide(libraries.get('auth'))] },
        setup: () => ({}),
      }),
    /incompatible/,
  );
  assert.equal(editorForwards, forwards, 'Invalid parent bindings refuse before any child forwarding runs.');
  assert.throws(
    () =>
      createCoreBusApi({
        name: 'undeclared-parent',
        libraries: { editor },
        libraryBindings: { editor: (_libraries, parent) => [editorAuth.provide(parent.require(cmsAuth))] },
        setup: () => ({}),
      }),
    /Undeclared required binding: cms.auth/,
  );
  assert.throws(
    () =>
      createCoreBusApi({
        name: 'undeclared-token',
        requires: [cmsAuth],
        bindings: [cmsAuth.provide(null)],
        libraries: { editor },
        // Holding a value of the same type is not a declaration of that capability token.
        libraryBindings: { editor: (_libraries, parent) => [editorAuth.provide(parent.require(foreign))] },
        setup: () => ({}),
      }),
    /Undeclared required binding: foreign.auth/,
  );
  let retained: RequiredBindings | undefined;
  createCoreBusApi({
    name: 'retained-parent',
    requires: [cmsAuth],
    bindings: [cmsAuth.provide(null)],
    libraries: { editor },
    libraryBindings: {
      editor: (_libraries, parent) => {
        retained = parent;
        return [editorAuth.provide(parent.require(cmsAuth))];
      },
    },
    setup: () => ({}),
  });
  assert.throws(() => retained?.require(cmsAuth), /resolved only during library setup/);
  assert.equal(authInitializations, initializations);
});

await test('cycles through parent-forwarding siblings refuse', () => {
  assert.throws(
    () =>
      createCoreBusApi({
        name: 'cyclic-cms',
        requires: [cmsAuth],
        bindings: [cmsAuth.provide(null)],
        libraries: { media, editor },
        libraryBindings: {
          editor: (libraries, parent) => {
            libraries.get('media');
            return [editorAuth.provide(parent.require(cmsAuth))];
          },
          media: (libraries, parent) => {
            libraries.get('editor');
            return [cmsAuth.provide(parent.require(cmsAuth))];
          },
        },
        setup: () => ({}),
      }),
    /Cyclic library bindings at 'cyclic-cms\//,
  );
});

await test('a same-wave command and expiry share the exact auth state and refuse protected execution', async () => {
  const bus = host.createBus({ scheduler: new ManualScheduler() });
  try {
    const session = host.getBus(bus).library('auth').exports;
    const model = editor.getBus(bus).exports;
    const admissions: [number, boolean][] = [];
    bus.listen(model.save, (requestId, admitted) => admissions.push([requestId, admitted]));
    let calls = 0;
    bindEffect(bus, model.effect, { execute: () => ++calls, failure: () => -1 });
    bus.publish(session.granted, null);
    bus.flush();
    // Admitted while reducing, but the batch-final expiry prevents planning and execution.
    bus.publish(model.save, 1);
    bus.publish(session.expired, null);
    await bus.drain();
    bus.publish(session.granted, null);
    bus.flush();
    // Expiry reduces first, so admission itself refuses.
    bus.publish(session.expired, null);
    bus.publish(model.save, 2);
    await bus.drain();
    assert.deepEqual(admissions, [
      [1, true],
      [2, false],
    ]);
    assert.equal(calls, 0);
    assert.equal(bus.read(model.saves), 0);
  } finally {
    bus.dispose();
  }
});

await test('bus creation, publication and reads never rerun forwarding or replace references', () => {
  const forwards = editorForwards;
  const hosts = hostForwards;
  const reference = host.createBus({ scheduler: new ManualScheduler() });
  const model = editor.getBus(reference).exports;
  const session = host.getBus(reference).library('auth').exports;
  reference.dispose();
  for (let index = 0; index < 64; index++) {
    const bus = host.createBus({ scheduler: new ManualScheduler() });
    try {
      const root = host.getBus(bus);
      const access = root.library('cms').library('editor');
      assert.equal(access.exports, model);
      assert.equal(access.exports.access, session);
      assert.equal(editor.getBus(bus), access);
      assert.equal(root.library('auth').exports, session);
      bus.publish(session.granted, null);
      bus.publish(model.save, index);
      bus.flush();
      assert.equal(access.read(session.valid), true);
      assert.equal(access.read(model.saves), 0);
    } finally {
      bus.dispose();
    }
  }
  assert.equal(editorForwards, forwards);
  assert.equal(hostForwards, hosts);
});

const browser = new Window({ url: 'https://parent-bindings.example.test/' });
const saved = new Map<string, PropertyDescriptor | undefined>();
for (const [key, value] of Object.entries({
  window: browser,
  document: browser.document,
  HTMLElement: browser.HTMLElement,
  Node: browser.Node,
  IS_REACT_ACT_ENVIRONMENT: true,
})) {
  saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
  Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
}
try {
  const { createRoot } = await import('react-dom/client');
  await test('nested React providers resolve the feature under the selected CMS occurrence', async () => {
    const bus = portal.createBus({ scheduler: new ManualScheduler() });
    const root = portal.getBus(bus);
    const staff = root.library('staff');
    const partner = root.library('partner');
    const scopes: ReturnType<typeof editor.getBus>[] = [];
    const useCanSave = editor.createSelectionHook(
      'editor.can-save',
      (state, model) => model.access !== null && state.read(model.access.valid),
      (model) => (model.access ? [model.access.valid] : []),
    );
    function EditorView() {
      scopes.push(editor.useBus());
      return h('output', null, `${useCanSave(null)}`);
    }
    const node = document.createElement('div');
    const view = createRoot(node);
    const render = (scope: typeof staff) =>
      act(() =>
        view.render(h(StrictMode, null, h(portal.Provider, { bus }, h(cms.Provider, { scope }, h(EditorView))))),
      );
    try {
      await render(staff);
      assert.equal(scopes.at(-1), staff.library('editor'));
      assert.equal(node.textContent, 'false');
      await act(() => {
        bus.publish(root.library('staffAuth').exports.granted, null);
        bus.flush();
      });
      assert.equal(node.textContent, 'true');
      await render(partner);
      assert.equal(scopes.at(-1), partner.library('editor'));
      assert.equal(scopes.at(-1)?.exports.access, root.library('partnerAuth').exports);
      assert.equal(node.textContent, 'false');
    } finally {
      await act(() => view.unmount());
      bus.flush();
      assert.deepEqual(bus.interestSource.snapshot(), []);
      bus.dispose();
    }
  });
} finally {
  await browser.happyDOM.close();
  for (const [key, descriptor] of saved) {
    if (descriptor) Object.defineProperty(globalThis, key, descriptor);
    else Reflect.deleteProperty(globalThis, key);
  }
}

function negativeTypes(): void {
  const text = defineCapability<string>('text');
  createCoreBusApi({
    name: 'typed-parent',
    requires: [cmsAuth, text],
    bindings: [cmsAuth.provide(null), text.provide('')],
    libraries: { editor },
    libraryBindings: {
      editor: (_libraries, parent) => {
        // @ts-expect-error Library callbacks cannot seal the parent occurrence.
        parent.seal();
        // @ts-expect-error A forwarded parent value retains its exact capability type.
        editorAuth.provide(parent.require(text));
        return [editorAuth.provide(parent.require(cmsAuth))];
      },
    },
    setup: () => ({}),
  });
  createCoreBusApi({
    name: 'typed-alias',
    requires: [cmsAuth],
    bindings: [cmsAuth.provide(null)],
    libraries: { editor },
    libraryBindings: {
      // @ts-expect-error The parent resolver is not itself a binding list or an inheritance alias.
      editor: (_libraries, parent) => parent,
    },
    setup: () => ({}),
  });
}
void negativeTypes;
console.log(JSON.stringify({ parentBindingScenarios: passed, builtExports: true }));
