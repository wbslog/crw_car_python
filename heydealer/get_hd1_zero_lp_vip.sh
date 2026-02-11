#!/bin/sh

export TODAY=`date "+%Y%m%d"`

export EXEC_FILE_DIR="/data/niffler_hd/bin"
export LOG_FILE_DIR="/data/niffler_hd/log"


# profile setting
source /data/niffler_hd/crawler_env/bin/activate

export ps_count=`ps -ef | grep HD_zero_1_LP_VIP | grep python | grep py | grep LP-VIP-PAGE | wc -l`

if [ $ps_count -eq "0" ]; then
        echo "Start niffler-hd-LP-VIP Used Engine"
	python -u $EXEC_FILE_DIR/HD_zero_1_LP_VIP.py LP-VIP-PAGE >> $LOG_FILE_DIR/niffler_hd_lp_vip$TODAY.log
else
        echo "Already niffler-hd-LP-VIP Used Engine Started"
fi


