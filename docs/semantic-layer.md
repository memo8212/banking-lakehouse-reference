# Semantic Layer & Genie

A thin **semantic layer** over the gold marts defines the business metrics, their
allowed dimensions and synonyms. It drives three things from one source of truth
(`semantic/metrics.yaml`):

1. **SQL for BI** — deterministic metric SQL (`python -m semantic sql …`).
2. **A Databricks AI/BI Genie space** — trusted tables, instructions, sample
   questions and metric SQL snippets (`python -m semantic genie`).
3. **Natural-language "Ask"** — match a question to a metric and execute it
   (`python -m semantic ask …`, or the Streamlit **Ask** app).

## The model (`semantic/metrics.yaml`)

```yaml
catalog: bank_dev
metrics:
  - name: total_card_spend
    description: Total posted card spend
    table: gold.card_spend_by_segment
    measure: "SUM(spend)"
    dimensions: [segment, month]
    synonyms: [card spend, card spending, spending]
    question: "What is card spend by segment?"
```

13 metrics ship across customer, cards, payments, loans, accounts, risk and AML.

## Commands

```bash
python -m semantic metrics                          # list metrics
python -m semantic sql total_card_spend --by segment
python -m semantic genie                            # -> semantic/generated/genie_space.json
python -m semantic ask "what is card spend by segment?" --run
```

## Genie space

`python -m semantic genie` writes a config you can paste into / apply to a
Databricks **AI/BI Genie** space:

```json
{
  "name": "bank_gold_genie",
  "catalog": "bank_dev",
  "trusted_tables": ["bank_dev.gold.card_spend_by_segment", "..."],
  "instructions": "Answer using ONLY the trusted tables and defined metrics...",
  "sample_questions": ["What is card spend by segment?", "..."],
  "sql": { "total_card_spend": "SELECT SUM(spend) AS total_card_spend FROM ..." }
}
```

Genie uses the instructions + sample questions + trusted assets to ground its
text-to-SQL, so answers stay on the curated gold marts.

## Ask (offline-first)

`semantic/ask.py` scores a question against metric names, descriptions and
synonyms, picks the best metric and infers the `group by` dimension from the text
("… by segment"). An **LLM is used only if configured** (`OPENAI_API_KEY` or
`DATABRICKS_HOST`+`DATABRICKS_TOKEN`) to choose among metrics; the human sees the
chosen metric and SQL.

Run it as an app:

```bash
streamlit run app_genie.py
```

## Why a semantic layer
- **Consistency** — one definition of "card spend" for BI, Genie and ad-hoc.
- **Governance** — metrics point only at trusted gold tables (tied to `governance/`).
- **Portability** — the same YAML renders SQL for Databricks SQL, Genie, or a
  downstream BI tool.
