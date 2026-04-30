declare module 'tree-kill' {
  export default function treeKill(
    pid: number,
    signal?: NodeJS.Signals,
    callback?: (error?: Error | null) => void,
  ): void;
}
