// config.ts is excluded from configured roots but remains a program member
// through this import. Routing must not reject it for being absent from files.
export { isDeployConfig } from './config.ts';
