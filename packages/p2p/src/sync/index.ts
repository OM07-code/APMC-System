// Unit III — Synchronization module barrel
// All exports are pure-logic (no I/O) so they can be used in demo scripts
// without infrastructure.

// §1: Clock Synchronization
export {
  APMCReferenceClockServer,
  ClockManager,
  cristianSync,
  berkeleySync,
} from './clock-sync';
export type {
  CristianSyncResult,
  BerkeleyRoundResult,
  BerkeleyNode,
  ClockCorrection,
  ClockReading,
} from './clock-sync';

// §2: Logical Clocks (Lamport + Vector)
export {
  LamportClock,
  VectorClock,
  LogicalClockManager,
} from './logical-clocks';
export type { VectorTimestamp, CausalRelation, LamportStampedMessage, VectorStampedMessage } from './logical-clocks';

// §3: Global State (Chandy–Lamport Snapshot)
export {
  MandiSnapshotNode,
  GlobalSnapshotCoordinator,
} from './global-state';
export type {
  MandiLocalState,
  ChannelMessage,
  ProcessSnapshot,
  GlobalSnapshot,
} from './global-state';

// §4 supplement: Beacon Protocol
export { BeaconManager } from './beacon';
export type { BeaconFrame, BeaconStats } from './beacon';

// §5: Mutual Exclusion
export {
  RicartAgrawalaMutex,
  LodhaKshemkalyaniMutex,
} from './mutual-exclusion';
export type { MutexMessage, MutexStats } from './mutual-exclusion';

// §6 Self-Study: Knapp's Deadlock Detection
export {
  LocalWFG,
  PathPushingDetector,
  EdgeChasingDetector,
  DiffusingComputationDetector,
  GlobalStateDeadlockDetector,
  KnappCategories,
} from './deadlock-detection';
export type { WFGEdge, CMHProbe, EchoQuery, EchoReply, TWFPath } from './deadlock-detection';
