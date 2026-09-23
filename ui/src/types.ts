export type PipelineItem = { id: string; label: string; status: string; detail?: string };

export type StatusPayload = {
  environment: string;
  database: string;
  streaming: string;
  debezium: string;
  amqStreams: string;
  ruleEngine: string;
  pipeline: PipelineItem[];
  mode: string;
  user: { name: string; role: string };
  timestamp: string;
};

export type Kpis = {
  transactionsToday: number;
  transactionsDelta: number;
  activePlayers: number;
  activePlayersDelta: number;
  alertsToday: number;
  alertsDelta: number;
  criticalAlerts: number;
  criticalDelta: number;
  openCases: number;
  openCasesDelta: number;
  sarsPending: number;
  sarsDelta: number;
  source: string;
};

export type StreamEvent = {
  eventId: string;
  time: string;
  createdAt?: string;
  playerId: string;
  amount: number;
  action: string;
  location: string;
  status: string;
  riskScore?: number;
};

export type AlertItem = {
  alertId: string;
  playerId: string;
  playerName: string;
  alertType: string;
  priority: string;
  riskScore: number;
  explanation: string;
  matchedRules: string[];
  status: string;
  createdAt?: string;
};

export type Customer = {
  playerId: string;
  name: string;
  country: string;
  riskScore: number;
  monthVolume: number;
  tier: string;
};

export type CaseItem = {
  caseId: string;
  playerId: string;
  playerName: string;
  title: string;
  priority: string;
  status: string;
  assignee: string | null;
  createdAt?: string;
};
