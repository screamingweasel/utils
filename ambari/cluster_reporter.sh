OUT="$(mktemp)"
python cluster_exporter.py --target localhost:8080 --cluster kp1acidctohdicluster --user admin  --output $OUT --https False
python cluster_reporter.py $OUT LAND
rm -f $OUTmoun
