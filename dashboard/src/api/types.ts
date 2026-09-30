import type { components } from "./schema.gen";

type Schemas = components["schemas"];

export type Summary = Schemas["Summary"];
export type TimeSeries = Schemas["TimeSeries"];
export type TimeSeriesPoint = Schemas["TimeSeriesPoint"];
export type ServiceStats = Schemas["ServiceStats"];
export type StoredEvent = Schemas["StoredEvent"];
export type EventPage = Schemas["EventPage"];
export type LiveTick = Schemas["LiveTick"];
export type ApiErrorBody = Schemas["Error"];
export type StatusClass = "2xx" | "3xx" | "4xx" | "5xx";
