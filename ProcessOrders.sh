#!/bin/bash

rdom () { local IFS=\> ; read -d \< E C ;}
echo_time() 
{ 
date +" [ %Y-%m-%d %H:%M:%S ] $(printf "%s " "$@" | sed 's/%/%%/g')"
}

OrderDir=/var/www/vhosts/geoinsight.xyz/cloud.geoinsight.xyz/nextcloud/data/Michael/files/NOAA_Germany
#OrderDir=/media/sf_CODE/Git_Hosteurope/noaa.geoinsight.xyz/noaa

OrderFile="Orders.txt"

OrderDestination=/var/www/vhosts/geoinsight.xyz/noaa.geoinsight.xyz/NOAA
#OrderDestination=/home/michael/NOAA

OrderNumber=""
NextOrderNumber=""
OrderLocation="" 
OrderStatus=""
Counter=0
MaxDestinationFolder=100 #in GB
BUSY=0

echo ""
echo "                **************************************"
echo "                ***  CRON at $(date '+%Y-%m-%d %H:%M:%S')   ***"
echo "                **************************************"
echo ""


while read line; do    
a=($line)    
OrderNumber=${a[0]}
OrderLocation=${a[1]} 
OrderStatus=${a[2]}
Counter=$((Counter+1))

if ! [[ $OrderNumber =~ ^[0-9]+$  && \
	( ${OrderStatus:0:8} == 'DOWNLOAD' || \
		${OrderStatus:0:10} == 'PROCESSING' || \
		${OrderStatus:0:8} == 'FINISHED' || \
		${OrderStatus:0:6} == 'TOOBIG' || \
		${OrderStatus:0:7} == 'DELETED' || \
		${OrderStatus:0:6} == 'DELETE') && \
	( ${OrderLocation:0:4} == 'ncdc' || \
		${OrderLocation:0:4} == 'ngdc')	]]
	then
	echo_time "ERROR: Check line $Counter: $OrderNumber $OrderLocation $OrderStatus"
	continue
	
fi

	#DELETED -> continue
	if [[ ${OrderStatus:0:7} == 'DELETED' ]]
		then
		continue
	fi

	#DELETE -> RM
	if [[ ${OrderStatus:0:6} == 'DELETE' ]]
		then
		echo_time "$OrderNumber is flagged with DELETE. This will remove folder $OrderNumber in $OrderDestination"
		rm -rf $OrderDestination/$OrderNumber
		sed -i -e "s/$OrderNumber $OrderLocation DELETE/$OrderNumber $OrderLocation DELETED/g" $OrderDir/$OrderFile
	fi

	#TOOBIG -> CONTINUE
	if [[ ${OrderStatus:0:6} == 'TOOBIG' ]]
		then
		echo_time "$OrderNumber is too large. Set flag to DELETE and reorder from NOAA."
		continue
	fi

	#DOWNLOAD -> Get NextOrderNumber
	if [[ ( ${OrderStatus:0:8} == 'DOWNLOAD') && \
		( -z "$NextOrderNumber" ) ]]
		then
		NextOrderNumber=$OrderNumber
	fi

	#FINISHED -> CONTINUE
	if [[ ${OrderStatus:0:8} == 'FINISHED' ]]
		then
		echo_time "$OrderNumber already FINISHED. Consider setting flag to DELETE."
		#LASTLINE -> EXIT/BREAK IF NO ORDER 
		if [[ ("$(cat $OrderDir/$OrderFile | wc -l)" -eq "$Counter") && \
			( -z $NextOrderNumber) ]]
			then
			echo_time "Nothing to do."
			exit
		fi
	else
		continue
	fi

	#PROCESSING -> CONTINUE
	if [[ ${OrderStatus:0:10} == 'PROCESSING' ]]
		then
		BUSY=1
		continue
	fi
done < $OrderDir/$OrderFile

#BUSY -> EXIT
OrderNumber=$NextOrderNumber
if [[ $BUSY == 1 ]]
	then
	echo_time "$OrderNumber is still processing! EXIT"	
	exit
fi

#GO AHEAD
echo_time "Next up is $OrderNumber because the status is $OrderStatus"

#IS SPACE LEFT?
OrderDestinationSize=$(du -sb $OrderDestination/ | cut -f1)
if [[ "$OrderDestinationSize" -gt  "$(( $MaxDestinationFolder * $((1024**3)) ))" ]]
	then 
		echo_time "ERROR: Folder is full! Current size is $(( $OrderDestinationSize/$((1024**3)) )) GB. Limit is $MaxDestinationFolder GB"
		echo_time "MSG: Make Space by setting an order to DELETE"
		exit
	else
		echo_time "Still" $(( $(( $MaxDestinationFolder - $(( $OrderDestinationSize / $((1024**3)) )) )) )) "GB free"
fi

#SET FLAG TO PROCESSING
sed -i -e "s/$OrderNumber $OrderLocation $OrderStatus/$OrderNumber $OrderLocation PROCESSING/g" $OrderDir/$OrderFile

#DOWNLOAD MANIFEST WITH wget
wget -N -P $OrderDestination/$OrderNumber "ftp://ftp.class.$OrderLocation.noaa.gov/$OrderNumber/*.xml"
manifest=$(ls $OrderDestination/$OrderNumber/CLASS-order-manifest.$OrderNumber.001.*.xml)

#TEST IF ORDER IS TOO BIG
if [[ -f $manifest ]]; then
	echo_time "Manifest file is there!"
	OrderSize=0
	while rdom; do
		if [[ $E = file_size ]]; then
			OrderSize=$(($OrderSize+$C))
		fi
	done < $manifest

	if [[ $OrderSize -gt $(( $MaxDestinationFolder * $((1024**3)))) ]]; then
	echo_time "Total order size is $OrderSize. That is $(($OrderSize-$(( $MaxDestinationFolder * $((1024**3)))))) GB too large"
	sed -i -e "s/$OrderNumber $OrderLocation PROCESSING/$OrderNumber $OrderLocation TOOBIG/g" $OrderDir/$OrderFile
	exit
fi

else
	sed -i -e "s/$OrderNumber $OrderLocation PROCESSING/$OrderNumber $OrderLocation FINISHED with Error (no manifest)/g" $OrderDir/$OrderFile
	echo_time "Include the manifest file in your order"
	exit
fi


FileName=""
while rdom; do
	if [[ $E = file_name ]]; then
		FileName=$C
		wget -N -P $OrderDestination/$OrderNumber "ftp://ftp.class.$OrderLocation.noaa.gov/$OrderNumber/001/$FileName"
	fi
	
	if [[ $E = checksum ]] ; then
		md5=($(md5sum $OrderDestination/$OrderNumber/$FileName))
		if [[ $C != $md5 ]]
			then
			echo_time "$FileName from Order $OrderNumber has a wrong md5 hash $md5" >> $OrderDestination/$OrderNumber/DownloadErrors.txt 
		fi
	fi
done < $manifest


#PRODUCE ERROR FILE
if [ -f $OrderDestination/$OrderNumber/DownloadErrors.txt ]
	then
	sed -i -e "s/$OrderNumber $OrderLocation PROCESSING/$OrderNumber $OrderLocation FINISHED with Errors/g" $OrderDir/$OrderFile
	echo_time "Download finished with Errors"
	cat $OrderDestination/$OrderNumber/DownloadErrors.txt
else
	sed -i -e "s/$OrderNumber $OrderLocation PROCESSING/$OrderNumber $OrderLocation FINISHED/g" $OrderDir/$OrderFile
	echo_time "Download FINISHED, ByeBye."
fi