import { StateBus } from '@smoothbricks/statebus-core';

/**
 * Browser-optimized StateBus that uses requestAnimationFrame
 * to batch event dispatching until after the next paint.
 */
export class AnimationFrameStateBus extends StateBus {
  private nextFrameId = 0;

  protected scheduleDispatch(): void {
    if (this.nextFrameId === 0) {
      this.nextFrameId = requestAnimationFrame(() => {
        this.dispatchEvents();
        this.nextFrameId = 0;
      });
    }
  }
}
