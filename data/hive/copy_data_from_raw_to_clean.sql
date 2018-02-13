Copy data and partition on the fly
insert overwrite table hmda_lar_snappy partition (as_of_year) select * from hmda_lar;