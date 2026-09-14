import {
  composeLibraries,
  defineLibrary,
  mountLibrary,
  type ComposedRuntime,
  type EventHandle,
  type LibraryDefinition,
  type LibraryScope,
  type MountedLibrary,
  type RuntimeOptions,
  type StateBusComposition,
  type StateReader,
} from './composition.js';
import {
  createCaptureEnvelope,
  migrateCaptureEnvelope,
  replayCaptureEnvelope,
  type CaptureEnvelope,
  type CaptureEnvelopeOptions,
} from './capture-envelope.js';
import type { EffectCodecDescriptor } from './effects.js';
import { replayScenario, type RecordedScenario } from './recording.js';

const apiIdentity = Symbol('StateBus API');
const accessIdentity = Symbol('StateBus access');

/** The existing live StateBus implementation, not a second store owned by an API. */
export type StateBusInstance = ComposedRuntime;
export type BusOptions = RuntimeOptions;

/** Opaque reference accepted as a child by either the core or React factory. */
export interface BusApiReference {
  readonly [apiIdentity]: ApiRecipe;
}
export type BusApiLibraries = Readonly<Record<string, BusApiReference>>;
export type BusApiExports<Api> = Api extends BusApi<infer Exports, infer _Libraries> ? Exports : never;
export type BusApiAccess<Api> = Api extends BusApi<infer Exports, infer Libraries>
  ? BusAccess<Exports, Libraries>
  : never;

/** Available only while declaring the parent. Child handles are already resolved and typed. */
export interface LibraryExports<Libraries extends BusApiLibraries> {
  get<Key extends keyof Libraries & string>(key: Key): BusApiExports<Libraries[Key]>;
}
export interface BusApiSpecification<Exports, Libraries extends BusApiLibraries> {
  readonly name: string;
  readonly version?: number;
  readonly previousVersions?: readonly number[];
  readonly requires?: LibraryDefinition<Exports>['requires'];
  readonly bindings?: Parameters<typeof mountLibrary>[2];
  readonly libraries?: Libraries;
  readonly setup: (builder: LibraryScope, libraries: LibraryExports<Libraries>) => Exports;
}

export interface BusAccessReference {
  readonly instance: StateBusInstance;
  readonly [accessIdentity]: ApiNode;
}
/** Stable access to one library occurrence in a shared bus. This does not own another store. */
export interface BusAccess<Exports, Libraries extends BusApiLibraries> extends BusAccessReference, StateReader {
  readonly exports: Exports;
  library<Key extends keyof Libraries & string>(key: Key): BusApiAccess<Libraries[Key]>;
  publish<T>(event: EventHandle<T>, payload: NoInfer<T>): void;
  publisher<T>(event: EventHandle<T>): (payload: T) => void;
}
export interface BusApi<Exports, Libraries extends BusApiLibraries = Record<never, never>> extends BusApiReference {
  readonly name: string;
  createBus(options?: BusOptions): StateBusInstance;
  /** Resolve this API within an optional explicit occurrence. Repeated ambiguous occurrences refuse. */
  getBus(instance: StateBusInstance, within?: BusAccessReference): BusAccess<Exports, Libraries>;
  replayScenario(scenario: RecordedScenario): StateBusInstance;
  createCaptureEnvelope(scenario: RecordedScenario, options: CaptureEnvelopeOptions): CaptureEnvelope;
  migrateCaptureEnvelope(envelope: CaptureEnvelope, options: CaptureEnvelopeOptions): CaptureEnvelope;
  replayCaptureEnvelope(envelope: CaptureEnvelope, effects?: readonly EffectCodecDescriptor[]): StateBusInstance;
}

interface ApiRecipe {
  readonly name: string;
  instantiate(namespace: string, declarations: MountedLibrary<unknown>[]): ApiNode;
}
interface ApiNode {
  readonly recipe: ApiRecipe;
  readonly model: unknown;
  readonly children: ReadonlyMap<string, ApiNode>;
  /** Precomputed at definition time, not a render-time traversal or path/string lookup. */
  readonly descendants: ReadonlyMap<ApiRecipe, ApiNode | null>;
  visible: ReadonlyMap<ApiRecipe, ApiNode | null>;
  access(instance: StateBusInstance): BusAccessReference;
}
interface ApiGraph {
  readonly root: ApiNode;
  readonly nodes: ReadonlySet<ApiNode>;
}
const graphs = new WeakMap<StateBusComposition, ApiGraph>();

function resolveNode(recipe: ApiRecipe, instance: StateBusInstance, within?: BusAccessReference): ApiNode {
  if (instance.disposed) throw new Error('Cannot access a disposed StateBus.');
  const graph = graphs.get(instance.composition);
  if (!graph) throw new Error('StateBus was not created from a bus API definition.');
  const scope = within?.[accessIdentity] ?? graph.root;
  if ((within && within.instance !== instance) || !graph.nodes.has(scope))
    throw new Error('A library selection belongs to a different StateBus.');
  const node = scope.visible.get(recipe);
  if (node === null) throw new Error(`Ambiguous bus API '${recipe.name}': select a library occurrence.`);
  if (!node) throw new Error(`Bus API '${recipe.name}' is not included in this StateBus.`);
  return node;
}

/**
 * Build the same reusable API for an application or a library. All declaration/configuration work
 * happens here; createBus alone allocates live state. The React package augments this factory with
 * providers/hooks without changing its bus, ownership, reducers, loader or replay implementation.
 */
export function createBusApi<Exports, Libraries extends BusApiLibraries = Record<never, never>>(
  specification: BusApiSpecification<Exports, Libraries>,
): BusApi<Exports, Libraries> {
  const { name, version, setup } = specification;
  if (!name) throw new Error('A bus API needs a name.');
  const requires = Object.freeze([...(specification.requires ?? [])]);
  const bindings = Object.freeze([...(specification.bindings ?? [])]);
  const previousVersions = Object.freeze([...(specification.previousVersions ?? [])]);
  const dependencies = new Map<string, ApiRecipe>();
  for (const [key, library] of Object.entries(specification.libraries ?? {})) {
    if (!key || !library?.[apiIdentity]) throw new Error('A library entry needs a name and a bus API.');
    dependencies.set(key, library[apiIdentity]);
  }

  const recipe: ApiRecipe = {
    name,
    instantiate(namespace, declarations) {
      const children = new Map<string, ApiNode>();
      for (const [key, dependency] of dependencies) {
        // Length-delimited paths are resolved only on this cold construction path.
        children.set(key, dependency.instantiate(`${namespace}${key.length}:${key}`, declarations));
      }
      function get<Key extends keyof Libraries & string>(key: Key): BusApiExports<Libraries[Key]>;
      function get(key: string): unknown {
        const child = children.get(key);
        if (!child) throw new Error(`Unknown library '${key}'.`);
        return child.model;
      }
      const libraryExports: LibraryExports<Libraries> = Object.freeze({ get });
      const definition = defineLibrary({
        name,
        version,
        previousVersions,
        requires,
        setup: (builder) => setup(builder, libraryExports),
      });
      const declaration = mountLibrary(definition, namespace, bindings);
      declarations.push(declaration);
      const accesses = new WeakMap<StateBusInstance, BusAccess<Exports, Libraries>>();
      const descendants = new Map<ApiRecipe, ApiNode | null>();
      const node: ApiNode = {
        recipe,
        model: declaration.exports,
        children,
        descendants,
        visible: descendants,
        access(instance) {
          const existing = accesses.get(instance);
          if (existing) return existing;
          instance.assertOwner(declaration.scope.ownerToken);
          function library<Key extends keyof Libraries & string>(key: Key): BusApiAccess<Libraries[Key]>;
          function library(key: string): unknown {
            const child = children.get(key);
            if (!child) throw new Error(`Unknown library '${key}'.`);
            if (instance.disposed) throw new Error('Cannot access a disposed StateBus.');
            return child.access(instance);
          }
          const access: BusAccess<Exports, Libraries> = Object.freeze({
            [accessIdentity]: node,
            instance,
            exports: declaration.exports,
            library,
            read: instance.reader.read,
            readKeyed: instance.reader.readKeyed,
            publish: <T>(event: EventHandle<T>, payload: NoInfer<T>) => instance.publish(event, payload),
            publisher: <T>(event: EventHandle<T>) => instance.publisher(event),
          });
          accesses.set(instance, access);
          return access;
        },
      };
      descendants.set(recipe, node);
      for (const child of children.values()) {
        for (const [identity, occurrence] of child.descendants) {
          descendants.set(identity, descendants.has(identity) ? null : occurrence);
        }
      }
      return node;
    },
  };
  Object.freeze(recipe);
  const declarations: MountedLibrary<unknown>[] = [];
  const root = recipe.instantiate(`${name.length}:${name}`, declarations);
  const nodes = new Set<ApiNode>();
  function resolveVisibility(node: ApiNode, inherited?: ReadonlyMap<ApiRecipe, ApiNode | null>): void {
    nodes.add(node);
    if (inherited) {
      const visible = new Map(inherited);
      for (const [identity, occurrence] of node.descendants) visible.set(identity, occurrence);
      node.visible = visible;
    }
    for (const child of node.children.values()) resolveVisibility(child, node.visible);
    Object.freeze(node);
  }
  resolveVisibility(root);
  const composition = composeLibraries(...declarations);
  graphs.set(composition, { root, nodes });
  function prepare(instance: StateBusInstance): StateBusInstance {
    try {
      // Access objects and bound functions are created once, before any component renders.
      for (const node of nodes) node.access(instance);
      return instance;
    } catch (cause) {
      instance.dispose();
      throw cause;
    }
  }
  function getBus(instance: StateBusInstance, within?: BusAccessReference): BusAccess<Exports, Libraries>;
  function getBus(instance: StateBusInstance, within?: BusAccessReference): BusAccessReference {
    return resolveNode(recipe, instance, within).access(instance);
  }
  return Object.freeze({
    [apiIdentity]: recipe,
    name,
    createBus: (options?: BusOptions) => prepare(composition.createRuntime(options)),
    getBus,
    replayScenario: (scenario: RecordedScenario) => prepare(replayScenario(composition, scenario)),
    createCaptureEnvelope: (scenario: RecordedScenario, options: CaptureEnvelopeOptions) =>
      createCaptureEnvelope(composition, scenario, options),
    migrateCaptureEnvelope: (envelope: CaptureEnvelope, options: CaptureEnvelopeOptions) =>
      migrateCaptureEnvelope(composition, envelope, options),
    replayCaptureEnvelope: (envelope: CaptureEnvelope, effects?: readonly EffectCodecDescriptor[]) =>
      prepare(replayCaptureEnvelope(composition, envelope, effects)),
  });
}
