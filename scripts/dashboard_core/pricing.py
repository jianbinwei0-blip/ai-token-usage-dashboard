from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

from .models import round_cost


BUILTIN_RATE_CARD = {
    "version": "2026-03-08",
    "providers": {
        "codex": {
            "gpt-5": {
                "input_per_million": 2.5,
                "output_per_million": 15.0,
                "cache_read_per_million": 0.25,
                "cache_write_per_million": 2.5,
            }
        },
        "pi": {
            "gpt-5": {
                "input_per_million": 2.5,
                "output_per_million": 15.0,
                "cache_read_per_million": 0.25,
                "cache_write_per_million": 2.5,
            }
        },
        "claude": {
            "claude-sonnet-4": {
                "input_per_million": 3.0,
                "output_per_million": 15.0,
                "cache_read_per_million": 0.3,
                "cache_write_per_million": 3.75,
            },
            "claude-fable-5": {
                "input_per_million": 3.0,
                "output_per_million": 15.0,
                "cache_read_per_million": 0.3,
                "cache_write_per_million": 3.75,
            },
            "claude-haiku-4": {
                "input_per_million": 0.8,
                "output_per_million": 4.0,
                "cache_read_per_million": 0.08,
                "cache_write_per_million": 1.0,
            },
            "claude-opus-4": {
                "input_per_million": 15.0,
                "output_per_million": 75.0,
                "cache_read_per_million": 1.5,
                "cache_write_per_million": 18.75,
            },
        },
    },
}


@dataclass(frozen=True)
class ModelRates:
    input_per_million: float
    output_per_million: float
    cache_read_per_million: float
    cache_write_per_million: float


@dataclass(frozen=True)
class CostBreakdown:
    input_cost_usd: float
    output_cost_usd: float
    cached_cost_usd: float
    total_cost_usd: float
    cost_complete: bool
    source: str

    @property
    def cost_status(self) -> str:
        return "complete" if self.cost_complete else "partial"


class PricingCatalog:
    def __init__(self, rate_card: dict, source: str = "built-in") -> None:
        self._rate_card = rate_card
        self.source = source
        self.version = str(rate_card.get("version") or "unknown")
        self._warnings: set[tuple[str, str]] = set()
        self._resolved_rates: dict[tuple[str, str], ModelRates | None] = {}

    @classmethod
    def from_file(cls, pricing_file: Path | None) -> "PricingCatalog":
        if pricing_file is None:
            return cls(BUILTIN_RATE_CARD, source="built-in")

        override = json.loads(pricing_file.read_text(encoding="utf-8"))
        merged = _copy_rate_card(BUILTIN_RATE_CARD)
        merged["version"] = override.get("version") or merged.get("version")
        merged_providers = merged.setdefault("providers", {})
        for provider, models in (override.get("providers") or {}).items():
            provider_entry = merged_providers.setdefault(provider, {})
            if isinstance(models, dict):
                provider_entry.update(models)
        return cls(merged, source=f"file:{pricing_file}")

    def resolve_rates(self, provider: str, model: str) -> ModelRates | None:
        cache_key = (provider, model)
        if cache_key in self._resolved_rates:
            return self._resolved_rates[cache_key]

        provider_models = (self._rate_card.get("providers") or {}).get(provider) or {}
        if not isinstance(provider_models, dict):
            self._resolved_rates[cache_key] = None
            return None

        best_match: tuple[int, dict] | None = None
        for pattern, rate_info in provider_models.items():
            if not isinstance(pattern, str) or not isinstance(rate_info, dict):
                continue
            if model == pattern or model.startswith(pattern):
                match = (len(pattern), rate_info)
                if best_match is None or match[0] > best_match[0]:
                    best_match = match

        if best_match is None:
            self._resolved_rates[cache_key] = None
            return None

        rate_info = best_match[1]
        rates = ModelRates(
            input_per_million=float(rate_info.get("input_per_million") or 0.0),
            output_per_million=float(rate_info.get("output_per_million") or 0.0),
            cache_read_per_million=float(rate_info.get("cache_read_per_million") or 0.0),
            cache_write_per_million=float(rate_info.get("cache_write_per_million") or 0.0),
        )
        self._resolved_rates[cache_key] = rates
        return rates

    def supports_exact_usage_aggregation(self, provider: str, model: str) -> bool:
        rates = self.resolve_rates(provider, model)
        if rates is None:
            return True
        for rate in (
            rates.input_per_million,
            rates.output_per_million,
            rates.cache_read_per_million,
            rates.cache_write_per_million,
        ):
            nanodollars_per_token = rate * 1_000.0
            if not math.isfinite(nanodollars_per_token) or not nanodollars_per_token.is_integer():
                return False
        return True

    def price_usage(
        self,
        provider: str,
        model: str,
        *,
        uncached_input_tokens: int,
        output_tokens: int,
        cache_read_tokens: int = 0,
        cache_write_tokens: int = 0,
        native_cost: dict[str, object] | None = None,
    ) -> CostBreakdown:
        native = self._native_cost_breakdown(native_cost)
        if native is not None:
            return native

        rates = self.resolve_rates(provider, model)
        if rates is None:
            if uncached_input_tokens == 0 and output_tokens == 0 and cache_read_tokens == 0 and cache_write_tokens == 0:
                return CostBreakdown(
                    input_cost_usd=0.0,
                    output_cost_usd=0.0,
                    cached_cost_usd=0.0,
                    total_cost_usd=0.0,
                    cost_complete=True,
                    source="zero-usage",
                )
            self._warnings.add((provider, model))
            return CostBreakdown(
                input_cost_usd=0.0,
                output_cost_usd=0.0,
                cached_cost_usd=0.0,
                total_cost_usd=0.0,
                cost_complete=False,
                source="unmapped",
            )

        input_cost_usd = round_cost((uncached_input_tokens * rates.input_per_million) / 1_000_000)
        output_cost_usd = round_cost((output_tokens * rates.output_per_million) / 1_000_000)
        cached_cost_usd = round_cost(
            (cache_read_tokens * rates.cache_read_per_million + cache_write_tokens * rates.cache_write_per_million)
            / 1_000_000
        )
        total_cost_usd = round_cost(input_cost_usd + output_cost_usd + cached_cost_usd)
        return CostBreakdown(
            input_cost_usd=input_cost_usd,
            output_cost_usd=output_cost_usd,
            cached_cost_usd=cached_cost_usd,
            total_cost_usd=total_cost_usd,
            cost_complete=True,
            source="derived",
        )

    def warnings(self) -> list[dict[str, str]]:
        return [
            {"provider": provider, "model": model}
            for provider, model in sorted(self._warnings)
        ]

    def metadata(self) -> dict[str, object]:
        warnings = self.warnings()
        return {
            "version": self.version,
            "source": self.source,
            "warnings": warnings,
            "warning_count": len(warnings),
        }

    def _native_cost_breakdown(self, native_cost: dict[str, object] | None) -> CostBreakdown | None:
        if not isinstance(native_cost, dict) or not native_cost:
            return None

        input_cost_usd = _safe_non_negative_float(native_cost.get("input"))
        output_cost_usd = _safe_non_negative_float(native_cost.get("output"))
        cache_read_cost = _safe_non_negative_float(native_cost.get("cacheRead"))
        cache_write_cost = _safe_non_negative_float(native_cost.get("cacheWrite"))
        total_cost_value = native_cost.get("total")
        cached_cost_usd = round_cost(cache_read_cost + cache_write_cost)

        if total_cost_value is None:
            total_cost_usd = round_cost(input_cost_usd + output_cost_usd + cached_cost_usd)
        else:
            total_cost_usd = round_cost(_safe_non_negative_float(total_cost_value))

        return CostBreakdown(
            input_cost_usd=round_cost(input_cost_usd),
            output_cost_usd=round_cost(output_cost_usd),
            cached_cost_usd=cached_cost_usd,
            total_cost_usd=total_cost_usd,
            cost_complete=True,
            source="native",
        )


def _copy_rate_card(rate_card: dict) -> dict:
    return json.loads(json.dumps(rate_card))


def _safe_non_negative_float(value: object) -> float:
    if isinstance(value, (int, float)) and value >= 0:
        return float(value)
    return 0.0
