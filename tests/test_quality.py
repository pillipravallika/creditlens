from creditlens import quality as Q


def test_quality_checks(spark):
    df = spark.createDataFrame([(1, "A", 10.0), (2, "B", None), (2, "Z", 99.0)], "id long, g string, x double")
    assert Q.not_null(df, "t", "x")["failed_rows"] == 1
    assert Q.unique(df, "t", ["id"])["failed_rows"] == 1
    assert Q.accepted_values(df, "t", "g", ["A", "B"])["failed_rows"] == 1
    assert Q.in_range(df, "t", "x", 0, 50)["failed_rows"] == 2        # null and 99 both fail
    parent = spark.createDataFrame([(1,), (2,)], "pk long")
    child = spark.createDataFrame([(1,), (3,)], "fk long")
    assert Q.relationships(child, parent, "t", "fk", "pk")["failed_rows"] == 1
    s = Q.summarize([Q.not_null(df, "t", "id"), Q.unique(df, "t", ["id"])])
    assert s["checks"] == 2 and s["failed"] == 1
