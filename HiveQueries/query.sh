cd "`dirname $0`"
PYPATH="./deps/:$PYTHONPATH" python ./run_query.py $@
