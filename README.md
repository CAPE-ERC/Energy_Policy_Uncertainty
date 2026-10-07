# Energy Policy Uncertainty (EgPU) Pipeline

## 1. Definition

EgPU means **Energy Policy Uncertainty**.

An article is counted only when its combined title and main article text contains all three separate components:

1. Energy
2. Policy
3. Uncertainty

The rule is **Energy AND Policy AND Uncertainty**. Economic terms are not required, and Trade terms are not required.

## 2. Location and files

The complete package is stored in:

    G:\My Drive\News_Articles_Africa_Workbook\EgPU

Main files:

- monthly_egpu_pipeline.py — complete baseline and monthly-update program
- energy_policy_uncertainty_keywords.json — multilingual 54-country keyword dictionary
- requirements.txt — required Python packages
- README.md — this guide
- egpu_outputs — created automatically when computation begins

The program reads the existing EPU-selected article files from:

    G:\My Drive\News_Articles_Africa_Workbook\Main_Monthly_Scrapper\Scrapped_Top_Articles

It does not scrape news and does not modify the article collection.

## 3. Why EgPU uses a separate dictionary

Policy and Uncertainty terms are copied from the canonical EPTU dictionary. Energy terms are maintained in the EgPU dictionary.

This keeps EgPU independent. The ETU and TPU frozen baselines include a fingerprint of their dictionary and matcher. Adding Energy terms to the shared EPTU JSON would change that fingerprint and make valid ETU and TPU baselines appear obsolete.

## 4. Countries and languages

The dictionary covers all 54 African countries and the same country aliases used by the EPU, ETU, and TPU framework.

It covers these 15 language codes:

- Afrikaans (af)
- Amharic (am)
- Arabic (ar)
- Seychellois Creole (crs)
- English (en)
- Spanish (es)
- French (fr)
- Malagasy (mg)
- Portuguese (pt)
- Kirundi (rn)
- Kinyarwanda (rw)
- Somali (so)
- Swahili (sw)
- Tigrinya (ti)
- Zulu (zu)

Each country keeps the canonical language assignment, Policy terms, and Uncertainty terms. Its Energy list is assembled from the languages assigned to that country.

The Energy component includes specific vocabulary for energy markets, electricity, grids, oil, gas, fuels, coal, renewables, solar, wind, hydropower, geothermal and nuclear energy, energy transition, energy security, tariffs, prices, generation, supply, infrastructure, refineries, energy pipelines, and storage.

Broad English words such as “power” and “pipeline” are not used alone. Specific phrases such as “power grid,” “power generation,” “oil pipeline,” and “gas pipeline” reduce false matches.

## 5. UTF-8 option

Keep the following option in every command:

    -X utf8

It makes Python read multilingual terms and article text consistently as UTF-8 on Windows. This is important for Arabic, Ethiopic scripts, accented French and Portuguese, and the other supported languages.

## 6. Check the setup without computing

Open PowerShell and run:

    cd "G:\My Drive\News_Articles_Africa_Workbook\EgPU"
    python -X utf8 monthly_egpu_pipeline.py validate 2026-09

Validation checks the input paths, all 54 countries, dictionary, GDP file, and requested date window. It does not scan all articles and does not create index results.

## 7. First run: build and freeze the historical baseline

Run this once:

    python -X utf8 monthly_egpu_pipeline.py baseline 2025-12

The historical baseline is locked to:

- Start: 2015-01
- End: 2025-12
- Coverage: all 54 countries

This is the one-time heavy run. It scans the eligible article files, builds the country-site-month panel, calibrates every country, freezes country parameters, and freezes the regional and Africa aggregate parameters.

Do not reuse an EPU, ETU, or TPU baseline for EgPU.

Do not reuse an older EgPU panel after changing keywords, aliases, language assignments, or matching logic.

If the full article scan completed and calibration was interrupted only for a non-methodological reason, the saved panel can be reused when its methodology fingerprint still matches:

    python -X utf8 monthly_egpu_pipeline.py baseline 2025-12 --reuse-panel

If the dictionary or matcher changed, do not use --reuse-panel. Rebuild from the article files.

## 8. Normal automatic monthly update

After the baseline completes successfully, compute all post-baseline months through a target month with one command:

    python -X utf8 monthly_egpu_pipeline.py auto 2026-09

For a later target, change only the date:

    python -X utf8 monthly_egpu_pipeline.py auto 2026-10

Auto mode does not rebuild the 2015–2025 history and does not scrape. It validates the frozen baseline, scans January 2026 through the requested target month, applies the frozen parameters, and refreshes country, regional, and Africa outputs.

## 9. Compute only one month

To scan and compute one post-baseline month only:

    python -X utf8 monthly_egpu_pipeline.py month 2026-10

Use month mode after earlier post-baseline results already exist. The newly computed month is merged with the existing output.

## 10. Optional country subset

Country subsets are allowed for post-baseline runs:

    python -X utf8 monthly_egpu_pipeline.py month 2026-10 --countries "Nigeria,Ghana"

Baseline mode must always use all 54 countries.

## 11. Outputs

All generated files remain under:

    G:\My Drive\News_Articles_Africa_Workbook\EgPU\egpu_outputs

Important outputs:

- egpu_index_long.csv — country-month EgPU in long format
- egpu_index_wide.csv — country-month EgPU in wide format
- egpu_monthly_panel_by_site.csv — site-level monthly counts
- egpu_monthly_panel_by_site.manifest.json — panel methodology fingerprint
- baseline_country_status.csv — country calibration coverage
- dropped_sites.csv — sites excluded from baseline calibration
- update_dropped_sites.csv — sites excluded during an update
- skipped_article_files.csv — unreadable or invalid source files
- latest_run_manifest.json — latest run details and fingerprint
- Regional_Output — regional and Africa CSVs and plots
- _egpu_baseline_cache — frozen baseline parameters and histories

## 12. Formula and aggregation structure

EgPU uses the same computational structure as the established EPU-derived pipelines:

1. Count qualifying EgPU articles for each newspaper site and month.
2. Divide the EgPU count by all eligible articles for that site-month.
3. Standardize each site series with moments estimated from 2015-01 through 2025-12.
4. Average the standardized available site series within each country-month.
5. Normalize each country series so its frozen historical mean is 100.
6. Standardize country indices for regional aggregation.
7. Create regional and Africa indices using monthly GDP-PPP weights and frozen aggregate normalization.

The defining content change is the first component: EgPU uses Energy instead of Economic or Trade.

## 13. Matching behavior

The shared matcher performs:

- Unicode NFKC normalization
- case-insensitive matching
- whole-word and whole-phrase boundaries
- matching across the title and main article text
- strict three-component logic

A single article must satisfy all three components. Terms inside one component are alternatives: any Energy term can satisfy Energy, any Policy term can satisfy Policy, and any Uncertainty term can satisfy Uncertainty.

Exact normalized keywords are disjoint across Energy, Policy, and Uncertainty, so the same exact term cannot satisfy more than one component.

## 14. Recommended run order

When all indices need updating, run them one at a time:

1. EPU
2. ETU
3. TPU
4. EgPU

All four read the same Google Drive article collection. Running several heavy scans simultaneously can make Google Drive much slower.

## 15. When the EgPU baseline must be rebuilt

Rebuild with the baseline command if any of these change:

- Energy, Policy, or Uncertainty keywords
- country names, aliases, or language assignments
- Unicode or phrase-matching rules
- the 2015–2025 baseline window
- the calibration or aggregation formula

A routine monthly update does not require rebuilding the baseline.

## 16. Troubleshooting

### Frozen baseline is missing or incomplete

Run:

    python -X utf8 monthly_egpu_pipeline.py baseline 2025-12

### Frozen baseline belongs to an older dictionary or matcher

The methodology fingerprint changed. Rebuild the baseline from article files. Do not use --reuse-panel.

### Some article files are unreadable

Inspect:

    egpu_outputs\skipped_article_files.csv

The scanner continues past isolated unreadable files. Investigate a large number or a concentration in one country, source, or month before accepting results.

### The scan is slow

The program uses the optimized lightweight CSV streaming reader, balanced file batches, and up to eight worker processes. Google Drive file latency is usually the limiting factor. Do not run the EPU, ETU, TPU, and EgPU heavy scans at the same time.

## 17. Research validation warning

EgPU follows the established mechanical structure of the EPU, ETU, and TPU pipelines, but its multilingual Energy vocabulary is a new research dictionary. Before publication, review samples of included and excluded articles by country and language for false positives and false negatives. Document every substantive keyword revision and rebuild the frozen baseline afterward.
