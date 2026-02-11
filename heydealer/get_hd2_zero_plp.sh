#!/bin/sh

export TODAY=`date "+%Y%m%d"`

export EXEC_FILE_DIR="/data/niffler_hd/bin"
export LOG_FILE_DIR="/data/niffler_hd/log"


# profile setting
source /data/niffler_hd/crawler_env/bin/activate

export ps_count=`ps -ef | grep HD_zero_2_PLP | grep python | grep py | grep PLP-PAGE | wc -l`

if [ $ps_count -eq "0" ]; then
        echo "Start niffler-hd-PLP Used Engine"
	python -u $EXEC_FILE_DIR/HD_zero_2_PLP.py PLP-PAGE >> $LOG_FILE_DIR/niffler_hd_plp_$TODAY.log
else
        echo "Already niffler-hd-PLP Used Engine Started"
fi


