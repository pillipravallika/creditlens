from creditlens.datagen import generate


def test_generator_is_reproducible_and_dirty():
    a = generate(n_customers=500, n_loans=800, seed=7)
    b = generate(n_customers=500, n_loans=800, seed=7)
    assert a["loans"].equals(b["loans"]) and a["payments"].equals(b["payments"])
    assert len(a["loans"]) > 800                                  # duplicates were injected
    assert a["loans"]["origination_date"].str.contains("/").any()  # mixed date formats present
    assert (a["loans"]["customer_id"] == "999999").any()           # orphan keys present


def test_clean_mode_has_no_mess():
    c = generate(n_customers=300, n_loans=500, seed=7, dirty=False)
    assert len(c["loans"]) == 500 and not c["loans"]["origination_date"].str.contains("/").any()
