# NPMM-labeling

A reference implementation for the paper ["A machine learning trading system for the stock market based on N-period Min-Max labeling using XGBoost"](https://doi.org/10.1016/j.eswa.2022.118581). The repository shows how to crawl NASDAQ-100 price data, generate multiple labeling schemes (Up/Down, N-day volatility, N-day barrier, and trade-action windows), and train models to predict future labels.

## Repository structure
- `code/1. Yahoo_Finance_Crawl.ipynb` – Crawls NASDAQ-100 price history from Yahoo Finance and saves daily OHLCV data.
- `code/2. NASDAQ_TA-Lib_Extraction.ipynb` – Derives technical indicators (TA-Lib) from the crawled data and writes them to `data/Stock_TI/`.
- `code/Labeling_code.R` – Core R functions for computing labels, selecting training instances, splitting data, and measuring trading metrics.
- `code/3. NASDAQ_Labeling.R` – Applies the labeling functions across all NASDAQ-100 symbols and stores the outputs in `data/Stock_Labeling/`.
- `code/4. Label_Prediction.R` – Trains classifiers (e.g., CatBoost, XGBoost) on technical indicators to predict labels and evaluates the resulting trading performance.
- `data/` – Example NASDAQ-100 price data, technical-indicator tables, and generated labels used by the scripts.

## Labeling methods
All labelers operate on adjusted closing prices and can optionally log intermediate calculations.
- **UpDown (`UpDown`)**: Marks positive N-step returns as 1 and negative as 2; default horizon is 1 day.【F:code/Labeling_code.R†L7-L18】
- **N-day volatility/return (`Nday_VDP`)**: Compares N-day log returns against dynamic upper/lower thresholds derived from the series mean and standard deviation; labels 1 (upper), 2 (lower), or 0 (neutral).【F:code/Labeling_code.R†L20-L52】
- **N-day barrier (`Nday_Barrier`)**: Looks ahead N days to see which barrier (upper or lower) is hit first and assigns 1 or 2 accordingly, or 0 if untouched.【F:code/Labeling_code.R†L54-L99】
- **Trade-action (`Trade_action`)**: Slides a window (e.g., 11 days) and labels the midpoint as a buy (1) if it is the local minimum, sell (2) if it is the local maximum, otherwise 0.【F:code/Labeling_code.R†L101-L131】

Utility helpers include `instance_selection()` for pruning duplicate sequential labels, `data_split()` for hold-out or time-series cross-validation splits, and `labeling_metrics()` for computing win rate, payoff ratio, profit factor, and cumulative profit for a labeled series.【F:code/Labeling_code.R†L133-L210】【F:code/Labeling_code.R†L212-L281】

## Workflow
1. **Download price data**: Run `code/1. Yahoo_Finance_Crawl.ipynb` to fetch NASDAQ-100 OHLCV data and save it under `data/`.
2. **Generate technical indicators**: Execute `code/2. NASDAQ_TA-Lib_Extraction.ipynb` to compute TA-Lib features, producing one CSV per symbol in `data/Stock_TI/`.
3. **Create labels**: From the `code/` directory, execute:
   ```r
   Rscript '3. NASDAQ_Labeling.R'
   ```
   This script reads `data/NASDAQ100.csv`, applies all labeling functions to each symbol, and writes the combined table to `data/Stock_Labeling/NASDAQ_labeling.csv`. The script will create the output directory if needed.【F:code/3. NASDAQ_Labeling.R†L1-L36】
4. **Train and evaluate models**: Install the listed R packages (e.g., `catboost`, `xgboost`, `gbm`, `lightgbm`, `adabag`, `e1071`), then run:
   ```r
   Rscript '4. Label_Prediction.R'
   ```
   The script merges technical indicators with labels, performs rolling time-series cross-validation, trains the selected model (`Use_Model`), and writes predictions to `data/test_prediction/` plus aggregated metrics to `data/ML_result/`.【F:code/4. Label_Prediction.R†L1-L125】【F:code/4. Label_Prediction.R†L164-L204】 Adjust the `exg` grid near the bottom of the script to explore different cross-validation periods or model choices.【F:code/4. Label_Prediction.R†L153-L204】

## Data expectations
- `data/NASDAQ100.csv` (and related CSVs) supply historical prices for each constituent symbol with column names ending in `Adj.Close`.
- `data/Stock_TI/` should contain per-symbol technical-indicator CSVs produced by the TA-Lib notebook.
- Running the labeling and prediction scripts will populate `data/Stock_Labeling/`, `data/train_instance/`, `data/test_prediction/`, and `data/ML_result/`.

## Notes
- The workflow assumes R 4.x with the listed packages; TA-Lib is required for feature extraction (a prebuilt Windows wheel is included at the repo root for convenience).
- Logging flags in `Labeling_code.R` can be toggled to emit per-method diagnostics (histograms, CSV traces) under a `Logging/` subdirectory.
- Example stock exclusions and model choices in `4. Label_Prediction.R` mirror the experimental setup from the paper; customize these lists to run on different universes or algorithms.
