# Databricks Practice

Reviewed synthetic Databricks notebook artifacts are published to dedicated review branches.

## Layout

Each folder under `notebooks/generated/<artifact_id>/` holds the Databricks source notebooks (`notebook.py` generates and validates, `read_delta.py` reads saved tables) plus the contract and review files. The Jupyter copies live in `exports/` (`generator_jupyter.ipynb`, `reader_jupyter.ipynb`): a Databricks Git folder cannot show a `.py` and an `.ipynb` notebook with the same name side by side, so open the `.py` files and treat the exports as portable copies.
