#!/bin/sh

function continue_ask {
        continue_flag="undef"
        while [ "$continue_flag" != "yes" -a "$continue_flag" != "no" ]
        do
                read -p "Type yes to continue or no to exit ...[yes|no]: " continue_flag
                if [ "$continue_flag" == "no" ]; then
                        exit 1;
                fi
        done
}

# arg1=start, arg2=end, format: %s.%N
function getTiming() {
    start=$1
    end=$2

    start_s=$(echo $start | cut -d '.' -f 1)
    start_ns=$(echo $start | cut -d '.' -f 2)
    end_s=$(echo $end | cut -d '.' -f 1)
    end_ns=$(echo $end | cut -d '.' -f 2)


# for debug..
#    echo $start
#    echo $end


    time=$(( ( 10#$end_s - 10#$start_s ) * 1000 + ( 10#$end_ns / 1000000 - 10#$start_ns / 1000000 ) ))


    echo "$time ms"
}

