import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard_core.pricing import PricingCatalog


class PricingTests(unittest.TestCase):
    def test_codex_pricing_derives_uncached_input_output_and_cached_cost(self) -> None:
        catalog = PricingCatalog.from_file(None)

        priced = catalog.price_usage(
            "codex",
            "gpt-5.4",
            uncached_input_tokens=150,
            output_tokens=100,
            cache_read_tokens=25,
        )

        self.assertTrue(priced.cost_complete)
        self.assertEqual(priced.source, "derived")
        self.assertAlmostEqual(priced.input_cost_usd, 0.000375)
        self.assertAlmostEqual(priced.output_cost_usd, 0.0015)
        self.assertAlmostEqual(priced.cached_cost_usd, 0.00000625)
        self.assertAlmostEqual(priced.total_cost_usd, 0.00188125)

    def test_gpt6_astra_pricing_covers_codex_pi_and_dsh(self) -> None:
        for provider in ("codex", "pi", "dsh"):
            for model in ("gpt-6-astra", "gpt-6-astra-2026-09-11"):
                with self.subTest(provider=provider, model=model):
                    catalog = PricingCatalog.from_file(None)
                    priced = catalog.price_usage(
                        provider,
                        model,
                        uncached_input_tokens=150,
                        output_tokens=100,
                        cache_read_tokens=25,
                        cache_write_tokens=20,
                    )

                    self.assertTrue(priced.cost_complete)
                    self.assertEqual(priced.source, "derived")
                    self.assertAlmostEqual(priced.input_cost_usd, 0.0015)
                    self.assertAlmostEqual(priced.output_cost_usd, 0.005)
                    self.assertAlmostEqual(priced.cached_cost_usd, 0.000275)
                    self.assertAlmostEqual(priced.total_cost_usd, 0.006775)
                    self.assertEqual(catalog.warnings(), [])

    def test_gpt6_astra_native_cost_is_not_replaced_by_standard_rates(self) -> None:
        catalog = PricingCatalog.from_file(None)
        priced = catalog.price_usage(
            "pi",
            "gpt-6-astra",
            uncached_input_tokens=150,
            output_tokens=100,
            cache_read_tokens=25,
            cache_write_tokens=20,
            native_cost={
                "input": 0.003,
                "output": 0.0075,
                "cacheRead": 0.00005,
                "cacheWrite": 0.0005,
                "total": 0.01105,
            },
        )

        self.assertTrue(priced.cost_complete)
        self.assertEqual(priced.source, "native")
        self.assertAlmostEqual(priced.input_cost_usd, 0.003)
        self.assertAlmostEqual(priced.output_cost_usd, 0.0075)
        self.assertAlmostEqual(priced.cached_cost_usd, 0.00055)
        self.assertAlmostEqual(priced.total_cost_usd, 0.01105)
        self.assertEqual(catalog.warnings(), [])

    def test_other_gpt6_models_are_not_assumed_to_use_astra_pricing(self) -> None:
        catalog = PricingCatalog.from_file(None)
        self.assertIsNone(catalog.resolve_rates("codex", "gpt-6-other"))

    def test_dsh_pricing_reuses_known_model_family_rates(self) -> None:
        catalog = PricingCatalog.from_file(None)

        priced = catalog.price_usage(
            "dsh",
            "gpt-5.6-sol",
            uncached_input_tokens=150,
            output_tokens=100,
            cache_read_tokens=25,
        )

        self.assertTrue(priced.cost_complete)
        self.assertEqual(priced.source, "derived")
        self.assertAlmostEqual(priced.total_cost_usd, 0.00188125)
        self.assertEqual(catalog.warnings(), [])

    def test_claude_pricing_derives_cache_write_and_cache_read_rollup(self) -> None:
        catalog = PricingCatalog.from_file(None)

        priced = catalog.price_usage(
            "claude",
            "claude-sonnet-4-6",
            uncached_input_tokens=10,
            output_tokens=177,
            cache_read_tokens=30,
            cache_write_tokens=20,
        )

        self.assertTrue(priced.cost_complete)
        self.assertEqual(priced.source, "derived")
        self.assertAlmostEqual(priced.input_cost_usd, 0.00003)
        self.assertAlmostEqual(priced.output_cost_usd, 0.002655)
        self.assertAlmostEqual(priced.cached_cost_usd, 0.000084)
        self.assertAlmostEqual(priced.total_cost_usd, 0.002769)

    def test_claude_fable_pricing_uses_sonnet_family_rate(self) -> None:
        catalog = PricingCatalog.from_file(None)

        priced = catalog.price_usage(
            "claude",
            "claude-fable-5",
            uncached_input_tokens=10,
            output_tokens=177,
            cache_read_tokens=30,
            cache_write_tokens=20,
        )

        self.assertTrue(priced.cost_complete)
        self.assertEqual(priced.source, "derived")
        self.assertAlmostEqual(priced.input_cost_usd, 0.00003)
        self.assertAlmostEqual(priced.output_cost_usd, 0.002655)
        self.assertAlmostEqual(priced.cached_cost_usd, 0.000084)
        self.assertAlmostEqual(priced.total_cost_usd, 0.002769)
        self.assertEqual(catalog.warnings(), [])

    def test_unmapped_zero_usage_is_complete_zero_cost(self) -> None:
        catalog = PricingCatalog.from_file(None)

        priced = catalog.price_usage(
            "claude",
            "<synthetic>",
            uncached_input_tokens=0,
            output_tokens=0,
            cache_read_tokens=0,
            cache_write_tokens=0,
        )

        self.assertTrue(priced.cost_complete)
        self.assertEqual(priced.source, "zero-usage")
        self.assertEqual(priced.total_cost_usd, 0.0)
        self.assertEqual(catalog.warnings(), [])

    def test_pi_native_cost_passthrough_is_preferred(self) -> None:
        catalog = PricingCatalog.from_file(None)

        priced = catalog.price_usage(
            "pi",
            "gpt-5.4",
            uncached_input_tokens=6444,
            output_tokens=81,
            cache_read_tokens=0,
            cache_write_tokens=0,
            native_cost={
                "input": 0.01611,
                "output": 0.001215,
                "cacheRead": 0.0,
                "cacheWrite": 0.0,
                "total": 0.017325,
            },
        )

        self.assertTrue(priced.cost_complete)
        self.assertEqual(priced.source, "native")
        self.assertAlmostEqual(priced.input_cost_usd, 0.01611)
        self.assertAlmostEqual(priced.output_cost_usd, 0.001215)
        self.assertAlmostEqual(priced.cached_cost_usd, 0.0)
        self.assertAlmostEqual(priced.total_cost_usd, 0.017325)

    def test_missing_pricing_marks_partial_and_records_warning(self) -> None:
        catalog = PricingCatalog.from_file(None)

        priced = catalog.price_usage(
            "claude",
            "totally-unknown-model",
            uncached_input_tokens=10,
            output_tokens=5,
            cache_read_tokens=2,
            cache_write_tokens=1,
        )

        self.assertFalse(priced.cost_complete)
        self.assertEqual(priced.source, "unmapped")
        self.assertEqual(priced.total_cost_usd, 0.0)
        self.assertEqual(
            catalog.warnings(),
            [{"provider": "claude", "model": "totally-unknown-model"}],
        )

    def test_override_file_merges_with_builtin_rate_card(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pricing_file = Path(tmpdir) / "pricing.json"
            pricing_file.write_text(
                '{"version":"custom-test","providers":{"codex":{"gpt-5":{"input_per_million":1.0,"output_per_million":2.0,"cache_read_per_million":0.1,"cache_write_per_million":1.0}}}}',
                encoding="utf-8",
            )
            catalog = PricingCatalog.from_file(pricing_file)

            priced = catalog.price_usage(
                "codex",
                "gpt-5.4",
                uncached_input_tokens=100,
                output_tokens=50,
                cache_read_tokens=20,
            )

            self.assertEqual(catalog.version, "custom-test")
            self.assertEqual(catalog.source, f"file:{pricing_file}")
            self.assertAlmostEqual(priced.input_cost_usd, 0.0001)
            self.assertAlmostEqual(priced.output_cost_usd, 0.0001)
            self.assertAlmostEqual(priced.cached_cost_usd, 0.000002)
            self.assertAlmostEqual(priced.total_cost_usd, 0.000202)
            self.assertEqual(catalog.resolve_rates("codex", "gpt-6-astra").output_per_million, 50.0)

    def test_gpt6_astra_specific_override_takes_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pricing_file = Path(tmpdir) / "pricing.json"
            pricing_file.write_text(
                '{"version":"custom-test","providers":{"dsh":{"gpt-6-astra":{"input_per_million":20.0,"output_per_million":100.0,"cache_read_per_million":2.0,"cache_write_per_million":25.0}}}}',
                encoding="utf-8",
            )
            catalog = PricingCatalog.from_file(pricing_file)
            priced = catalog.price_usage(
                "dsh",
                "gpt-6-astra-2026-09-11",
                uncached_input_tokens=150,
                output_tokens=100,
                cache_read_tokens=25,
                cache_write_tokens=20,
            )

            self.assertTrue(priced.cost_complete)
            self.assertAlmostEqual(priced.total_cost_usd, 0.01355)
            self.assertEqual(catalog.version, "custom-test")
            self.assertEqual(catalog.resolve_rates("pi", "gpt-6-astra").output_per_million, 50.0)


if __name__ == "__main__":
    unittest.main()
