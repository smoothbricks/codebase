# CLAUDE.md

## statebus Architecture

- **statebus-core**: Platform-agnostic state management
  - Abstract `StateBus` base class with `scheduleDispatch()` method
  - `ManualStateBus` for explicit dispatch control (testing)
  - No DOM dependencies
- **statebus-react**: React-specific implementation
  - `AnimationFrameStateBus` uses requestAnimationFrame for browser optimization
  - Default export is AnimationFrameStateBus
  - Also exports ManualStateBus for tests

## Important Notes

- Tests use ManualStateBus and call `dispatchEvents()` explicitly
- React production code uses AnimationFrameStateBus by default
