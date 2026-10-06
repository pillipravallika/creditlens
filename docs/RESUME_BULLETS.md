# Resume bullets (use ONLY what you have actually built and run)

Project line (Projects section):
**CreditLens: Loan Risk Lakehouse** | PySpark, Delta Lake, Databricks, SQL, Python, GitHub Actions | github.com/<you>/creditlens

Pick the bullets that are true after you finish each phase:

After Phase 1b (run on Databricks):
- Built a medallion lakehouse (bronze/silver/gold) on Databricks with PySpark and Delta Lake, processing ~780K payment
  records across 3 source files into a star schema and a loan-vintage risk mart.
- Designed a silver layer where every source row is either cleaned or routed to a rejects table with a reason code,
  enforcing an exact source-to-target tie-out (bronze = silver + rejects) across all tables.
- Implemented an automated data quality gate (not-null, uniqueness, accepted values, ranges, referential integrity)
  that fails the scheduled Databricks Job before bad data reaches the gold layer.
- Wrote 9+ unit tests on local Spark and a GitHub Actions CI pipeline that runs them on every pull request.

After Phase 1c:
- Implemented incremental ingestion with Auto Loader and idempotent Delta MERGE, and tracked customer history with SCD Type 2.

After Phase 2 (agent):
- Built an AI agent (LangGraph) that answers portfolio-risk questions by generating SQL over governed gold tables, with a
  RAG layer over credit-policy documents and an evaluation suite measuring answer accuracy.

Rules:
1. Say "synthetic data" in the README. Never describe the project as bank or employer work.
2. Do not copy any real Bank of America code, data, schemas, or internal details into this project.
3. If a bullet contains a number, you must be able to reproduce it live in an interview.
