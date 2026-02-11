#!/bin/sh

export TODAY=`date "+%Y%m%d"`

export EXEC_FILE_DIR="/data/niffler_hd/bin"
export LOG_FILE_DIR="/data/niffler_hd/log"


# profile setting
source /data/niffler_hd/crawler_env/bin/activate

export ps_count=`ps -ef | grep HD_zero_2_VIP | grep python | grep py | grep VIP-PAGE | wc -l`

if [ $ps_count -eq "0" ]; then
        echo "Start niffler-hd-VIP Used Engine"
	python -u $EXEC_FILE_DIR/HD_zero_2_VIP.py VIP-PAGE >> $LOG_FILE_DIR/niffler_hd_vip_$TODAY.log
else
        echo "Already niffler-hd-VIP Used Engine Started"
fi


