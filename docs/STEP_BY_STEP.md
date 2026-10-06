# Step-by-step: from zero to a running lakehouse

You said you know `git add / commit / push / pull`. That is enough to start. Do the steps in order and tick them off.
Expect about 10 to 12 hours for Phase 1b. Do not skip step 4: understanding your own tests is the point.

## 1. Accounts and tools (about 1 hour)
- [ ] GitHub account, then **New repository** named `creditlens` (public, so recruiters can see it). Do not add a README there.
- [ ] Install: Python 3.11, Java 17 (needed by Spark), Git, VS Code.
- [ ] Databricks Free Edition: sign up at databricks.com (search "Databricks Free Edition"). No credit card.
  It gives serverless compute only, supports Python and SQL (not Scala), and has limits on outbound internet.

## 2. Put the project on GitHub (30 min)
```bash
cd creditlens
git init
git add .
git commit -m "Initial CreditLens scaffold"
git branch -M main
git remote add origin https://github.com/<your-username>/creditlens.git
git push -u origin main
```
After the push, open the **Actions** tab on GitHub. The CI workflow runs your tests. A green tick is your first piece of evidence.

## 3. Run it locally first (1 hour)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q
python scripts/run_local.py
```
Read the output: the tie-out lines (`balanced: True`) and the reject reasons are the heart of the project.

## 4. Understand it (2 to 3 hours, the most important step)
Open `src/creditlens/transforms.py` and answer these in your own words (write them in `docs/NOTES.md`):
1. Why does silver keep a `rejects` table instead of just dropping bad rows?
2. What does `_flag_duplicates` do, and why order by `_ingest_ts` descending?
3. Why is `to_double` written with a regex check before the cast? (Hint: ANSI mode.)
4. In `build_fact_payments`, what does each window function compute? Why `rowsBetween(unboundedPreceding, currentRow)`?
5. What does `build_portfolio_risk_monthly` return, and how would a risk analyst read it?
Then break something on purpose (change a rule, run the tests, watch one fail, fix it). Ask me when stuck.

## 5. Run it on Databricks (2 to 3 hours)
1. In Databricks: **Workspace > Create > Git folder**, paste your GitHub repo URL (you may need to link GitHub in
   User Settings > Linked accounts first).
2. Open `notebooks/00_setup_and_generate`, attach **Serverless** compute, **Run all**.
   If your default catalog is not `workspace`, change the `catalog` widget at the top.
3. Run `01`, `02`, `03`, `04` in order. Read each markdown cell before running.
4. In `02`, confirm the tie-out lines print `balanced: True`.
5. Take screenshots of: the tables in Catalog Explorer, the tie-out, the grade chart from notebook 03. They go in the README.

## 6. Schedule it (1 to 2 hours)
- [ ] **Jobs & Pipelines > Create job** with 5 tasks (00 to 04, each depending on the previous one).
- [ ] Run it once. Then deliberately break a check (for example change an accepted status value), re-run, and
  watch the quality gate fail the job. A screenshot of that is a great interview story.

## 7. Add a dashboard (1 to 2 hours)
Create an AI/BI dashboard on `gold_portfolio_risk_monthly`: charge-off rate by grade, and a vintage heatmap.

## 8. Polish and publish (1 hour)
- [ ] Add screenshots to the README, tick the roadmap boxes you finished, and pin the repo on your GitHub profile.
- [ ] Add the resume bullets from `docs/RESUME_BULLETS.md` **only for what you actually completed**.
- [ ] Practise explaining the architecture out loud in 2 minutes.

## Working with Git properly (practice for CI/CD)
For every change from now on: `git switch -c feature/<name>`, commit, push, open a **pull request**, wait for the
CI check to go green, then merge. This is how real teams work and it is exactly what "CI/CD and Git" in job postings means.
