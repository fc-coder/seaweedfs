package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"io"
	"math"
	"sort"
)

func init() {
	Commands = append(Commands, &commandVolumePurge{})
}

type commandVolumePurge struct {
}

func (c *commandVolumePurge) Name() string {
	return "volume.purge"
}

func (c *commandVolumePurge) Help() string {
	return `volume.purge -keepCount=10 force , Keep a maximum of {keepCount} volumes in the system
	This command purge，Keep a maximum of {keepCount} volumes in the system. If the number exceeds, the earliest created file will be deleted
    If there is no {force} parameter, volumes will not be deleted, and only those files will be deleted
   `
}

func (c *commandVolumePurge) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	purgeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	keepCount := purgeCommand.Uint64("keepCount",math.MaxUint64 , "Keep a maximum of <keepCount> files in the system")
	if err = purgeCommand.Parse(args); err != nil {
		fmt.Fprintf(writer, "Error:%s\n",err.Error())
		return nil
	}
	force :=false
	for _,name :=range purgeCommand.Args(){
		if name =="force" {
			force=true
		}
	}
	topologyInfo, _, err := collectTopologyInfo(commandEnv)
	if err != nil {
		fmt.Fprintf(writer, "Error:%s\n",err.Error())
		return err
	}
	dataCenterInfo:=topologyInfo.DataCenterInfos
	volumeIdToVolumeServerList := make(map[uint32][]string)
	var volumeIds[] uint32
	var volumeServers[] string
	var volumeCount  uint64=0
	for _, dataCenter := range dataCenterInfo {
		if dataCenter.RackInfos == nil || len(dataCenter.RackInfos) == 0 {
			continue
		}
		for _, rack := range  dataCenter.RackInfos {
			if rack.DataNodeInfos == nil || len(rack.DataNodeInfos) == 0 {
				continue
			}
			for _, dataNode := range rack.DataNodeInfos {
				volumeServers=append(volumeServers, dataNode.Id)
				for _, disk := range dataNode.DiskInfos {
					if disk.VolumeInfos == nil || len(disk.VolumeInfos) == 0 {
						continue
					}
					for _, volume := range  disk.VolumeInfos{
						volumeCount++;
						if volumeServerList,ok := volumeIdToVolumeServerList[volume.Id]; !ok {
							volumeServerList=make([]string,0)
							volumeIdToVolumeServerList[volume.Id] =volumeServerList
						}
						volumeServerList:=append(volumeIdToVolumeServerList[volume.Id],dataNode.Id)
						volumeIdToVolumeServerList[volume.Id] =volumeServerList
						volumeIds = append(volumeIds, volume.Id)
					}
				}
			}
		}
	}
	deleteVolumeCount := volumeCount - *keepCount
	if deleteVolumeCount <=0{
		fmt.Fprintf(writer, "No need to do anything\n")
		return nil
	}
	if deleteVolumeCount > volumeCount{
		deleteVolumeCount = volumeCount
	}
	sort.Slice(volumeIds, func(i, j int) bool {
		if volumeIds[i] < volumeIds[j] {
			return true
		}
		return false
	})

	var hasDeleteCounter uint64=0
	for _,volumeId := range volumeIds{
		if hasDeleteCounter >=deleteVolumeCount{
			break
		}
		if force {
			volumeServerList := volumeIdToVolumeServerList[volumeId]
			err := deleteVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(volumeId), volumeServerList[0])
			if err != nil {
				fmt.Fprintf(writer,"ERROR! Node %s volumeId is %d  error:%s\n", volumeServerList[0], volumeId, err.Error())
				return err
			}
			fmt.Fprintf(writer, "Delete VolumeId:%d  Success\n",volumeId)
		}else{
			fmt.Fprintf(writer, "VolumeId:%d may be deleted\n",volumeId)
		}
		hasDeleteCounter++;
	}
	fmt.Fprintf(writer, "End of purge operation\n")
	return nil
}



