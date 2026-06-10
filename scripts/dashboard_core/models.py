from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field


BreakdownKey = tuple[str, str]
AttributionKey = tuple[str, str]
ActivityKey = tuple[dt.date, int]


def round_cost(value: float) -> float:
    return round(float(value), 9)


@dataclass
class BreakdownTotals:
    agent_cli: str
    model: str
    sessions: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0
    input_cost_usd: float = 0.0
    output_cost_usd: float = 0.0
    cached_cost_usd: float = 0.0
    total_cost_usd: float = 0.0
    cost_complete: bool = True

    @property
    def cost_status(self) -> str:
        return "complete" if self.cost_complete else "partial"


@dataclass
class AttributionTotals:
    category: str
    name: str
    sessions: int = 0
    events: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0
    input_cost_usd: float = 0.0
    output_cost_usd: float = 0.0
    cached_cost_usd: float = 0.0
    total_cost_usd: float = 0.0
    cost_complete: bool = True

    @property
    def cost_status(self) -> str:
        return "complete" if self.cost_complete else "partial"


@dataclass
class ActivityTotals:
    date: dt.date
    hour: int
    sessions: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0
    input_cost_usd: float = 0.0
    output_cost_usd: float = 0.0
    cached_cost_usd: float = 0.0
    total_cost_usd: float = 0.0
    cost_complete: bool = True

    @property
    def cost_status(self) -> str:
        return "complete" if self.cost_complete else "partial"

    def add_usage(
        self,
        *,
        sessions: int = 0,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cached_tokens: int = 0,
        total_tokens: int = 0,
        input_cost_usd: float = 0.0,
        output_cost_usd: float = 0.0,
        cached_cost_usd: float = 0.0,
        total_cost_usd: float = 0.0,
        cost_complete: bool | None = None,
    ) -> None:
        self.sessions += sessions
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        self.cached_tokens += cached_tokens
        self.total_tokens += total_tokens
        self.input_cost_usd = round_cost(self.input_cost_usd + input_cost_usd)
        self.output_cost_usd = round_cost(self.output_cost_usd + output_cost_usd)
        self.cached_cost_usd = round_cost(self.cached_cost_usd + cached_cost_usd)
        self.total_cost_usd = round_cost(self.total_cost_usd + total_cost_usd)
        if cost_complete is not None:
            self.cost_complete = self.cost_complete and cost_complete

    def merge_from(self, other: "ActivityTotals") -> None:
        self.add_usage(
            sessions=other.sessions,
            input_tokens=other.input_tokens,
            output_tokens=other.output_tokens,
            cached_tokens=other.cached_tokens,
            total_tokens=other.total_tokens,
            input_cost_usd=other.input_cost_usd,
            output_cost_usd=other.output_cost_usd,
            cached_cost_usd=other.cached_cost_usd,
            total_cost_usd=other.total_cost_usd,
            cost_complete=other.cost_complete,
        )


@dataclass
class DailyTotals:
    date: dt.date
    sessions: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0
    input_cost_usd: float = 0.0
    output_cost_usd: float = 0.0
    cached_cost_usd: float = 0.0
    total_cost_usd: float = 0.0
    cost_complete: bool = True
    breakdowns: dict[BreakdownKey, BreakdownTotals] = field(default_factory=dict, repr=False)
    attributions: dict[AttributionKey, AttributionTotals] = field(default_factory=dict, repr=False)

    @property
    def cost_status(self) -> str:
        return "complete" if self.cost_complete else "partial"

    def add_usage(
        self,
        *,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cached_tokens: int = 0,
        total_tokens: int = 0,
        input_cost_usd: float = 0.0,
        output_cost_usd: float = 0.0,
        cached_cost_usd: float = 0.0,
        total_cost_usd: float = 0.0,
        cost_complete: bool | None = None,
    ) -> None:
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        self.cached_tokens += cached_tokens
        self.total_tokens += total_tokens
        self.input_cost_usd = round_cost(self.input_cost_usd + input_cost_usd)
        self.output_cost_usd = round_cost(self.output_cost_usd + output_cost_usd)
        self.cached_cost_usd = round_cost(self.cached_cost_usd + cached_cost_usd)
        self.total_cost_usd = round_cost(self.total_cost_usd + total_cost_usd)
        if cost_complete is not None:
            self.cost_complete = self.cost_complete and cost_complete

    def add_breakdown(
        self,
        *,
        agent_cli: str,
        model: str,
        sessions: int = 0,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cached_tokens: int = 0,
        total_tokens: int = 0,
        input_cost_usd: float = 0.0,
        output_cost_usd: float = 0.0,
        cached_cost_usd: float = 0.0,
        total_cost_usd: float = 0.0,
        cost_complete: bool | None = None,
    ) -> None:
        key = (agent_cli, model)
        bucket = self.breakdowns.get(key)
        if bucket is None:
            bucket = BreakdownTotals(agent_cli=agent_cli, model=model)
            self.breakdowns[key] = bucket

        bucket.sessions += sessions
        bucket.input_tokens += input_tokens
        bucket.output_tokens += output_tokens
        bucket.cached_tokens += cached_tokens
        bucket.total_tokens += total_tokens
        bucket.input_cost_usd = round_cost(bucket.input_cost_usd + input_cost_usd)
        bucket.output_cost_usd = round_cost(bucket.output_cost_usd + output_cost_usd)
        bucket.cached_cost_usd = round_cost(bucket.cached_cost_usd + cached_cost_usd)
        bucket.total_cost_usd = round_cost(bucket.total_cost_usd + total_cost_usd)
        if cost_complete is not None:
            bucket.cost_complete = bucket.cost_complete and cost_complete

    def add_attribution(
        self,
        *,
        category: str,
        name: str,
        sessions: int = 0,
        events: int = 0,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cached_tokens: int = 0,
        total_tokens: int = 0,
        input_cost_usd: float = 0.0,
        output_cost_usd: float = 0.0,
        cached_cost_usd: float = 0.0,
        total_cost_usd: float = 0.0,
        cost_complete: bool | None = None,
    ) -> None:
        key = (category, name)
        bucket = self.attributions.get(key)
        if bucket is None:
            bucket = AttributionTotals(category=category, name=name)
            self.attributions[key] = bucket

        bucket.sessions += sessions
        bucket.events += events
        bucket.input_tokens += input_tokens
        bucket.output_tokens += output_tokens
        bucket.cached_tokens += cached_tokens
        bucket.total_tokens += total_tokens
        bucket.input_cost_usd = round_cost(bucket.input_cost_usd + input_cost_usd)
        bucket.output_cost_usd = round_cost(bucket.output_cost_usd + output_cost_usd)
        bucket.cached_cost_usd = round_cost(bucket.cached_cost_usd + cached_cost_usd)
        bucket.total_cost_usd = round_cost(bucket.total_cost_usd + total_cost_usd)
        if cost_complete is not None:
            bucket.cost_complete = bucket.cost_complete and cost_complete

    def merge_from(self, other: "DailyTotals") -> None:
        self.sessions += other.sessions
        self.add_usage(
            input_tokens=other.input_tokens,
            output_tokens=other.output_tokens,
            cached_tokens=other.cached_tokens,
            total_tokens=other.total_tokens,
            input_cost_usd=other.input_cost_usd,
            output_cost_usd=other.output_cost_usd,
            cached_cost_usd=other.cached_cost_usd,
            total_cost_usd=other.total_cost_usd,
            cost_complete=other.cost_complete,
        )
        for bucket in other.breakdowns.values():
            self.add_breakdown(
                agent_cli=bucket.agent_cli,
                model=bucket.model,
                sessions=bucket.sessions,
                input_tokens=bucket.input_tokens,
                output_tokens=bucket.output_tokens,
                cached_tokens=bucket.cached_tokens,
                total_tokens=bucket.total_tokens,
                input_cost_usd=bucket.input_cost_usd,
                output_cost_usd=bucket.output_cost_usd,
                cached_cost_usd=bucket.cached_cost_usd,
                total_cost_usd=bucket.total_cost_usd,
                cost_complete=bucket.cost_complete,
            )
        for bucket in other.attributions.values():
            self.add_attribution(
                category=bucket.category,
                name=bucket.name,
                sessions=bucket.sessions,
                events=bucket.events,
                input_tokens=bucket.input_tokens,
                output_tokens=bucket.output_tokens,
                cached_tokens=bucket.cached_tokens,
                total_tokens=bucket.total_tokens,
                input_cost_usd=bucket.input_cost_usd,
                output_cost_usd=bucket.output_cost_usd,
                cached_cost_usd=bucket.cached_cost_usd,
                total_cost_usd=bucket.total_cost_usd,
                cost_complete=bucket.cost_complete,
            )
