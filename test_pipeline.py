import pytest
import pandas as pd
from pipeline import extract_from_blob, transform


def test_extract_returns_data():
    df = extract_from_blob()
    assert len(df) > 0


def test_extract_row_count():
    df = extract_from_blob()
    assert len(df) == 1000


def test_extract_has_required_columns():
    df = extract_from_blob()
    assert 'ProductName' in df.columns
    assert 'BasePrice' in df.columns
    assert 'NegotiatedPrice' in df.columns


def test_transform_removes_nulls():
    df = extract_from_blob()
    df_clean = transform(df)
    assert df_clean.isnull().sum().sum() == 0


def test_transform_positive_prices():
    df = extract_from_blob()
    df_clean = transform(df)
    assert (df_clean['BasePrice'] > 0).all()
    assert (df_clean['NegotiatedPrice'] > 0).all()


def test_transform_adds_discount_column():
    df = extract_from_blob()
    df_clean = transform(df)
    assert 'DiscountPct' in df_clean.columns