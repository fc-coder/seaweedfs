package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"io"
	"regexp"
	"strings"
)

func init() {
	Commands = append(Commands, &commandRemoteConfigure{})
}

type commandRemoteConfigure struct {
}

func (c *commandRemoteConfigure) Name() string {
	return "remote.configure"
}

func (c *commandRemoteConfigure) Help() string {
	return `remote storage configuration

	# see the current configurations
	remote.configure

	# set or update a configuration
	remote.configure -name=cloud1 -type=s3 -access_key=xxx -secret_key=yyy

	# delete one configuration
	remote.configure -delete -name=cloud1

`
}

var (
	isAlpha = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`).MatchString
)

func (c *commandRemoteConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	conf := &filer_pb.RemoteConf{}

	remoteConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	isDelete := remoteConfigureCommand.Bool("delete", false, "delete one remote storage by its name")

	remoteConfigureCommand.StringVar(&conf.Name, "name", "", "a short name to identify the remote storage")
	remoteConfigureCommand.StringVar(&conf.Type, "type", "s3", "storage type, currently only support s3")

	remoteConfigureCommand.StringVar(&conf.S3AccessKey, "s3.access_key", "", "s3 access key")
	remoteConfigureCommand.StringVar(&conf.S3SecretKey, "s3.secret_key", "", "s3 secret key")
	remoteConfigureCommand.StringVar(&conf.S3Region, "s3.region", "us-east-2", "s3 region")
	remoteConfigureCommand.StringVar(&conf.S3Endpoint, "s3.endpoint", "", "endpoint for s3-compatible local object store")

	if err = remoteConfigureCommand.Parse(args); err != nil {
		return nil
	}

	if conf.Name == "" {
		return c.listExistingRemoteStorages(commandEnv, writer)
	}

	if !isAlpha(conf.Name) {
		return fmt.Errorf("only letters and numbers allowed in name: %v", conf.Name)
	}

	if *isDelete {
		return c.deleteRemoteStorage(commandEnv, writer, conf.Name)
	}

	return c.saveRemoteStorage(commandEnv, writer, conf)

}

func (c *commandRemoteConfigure) listExistingRemoteStorages(commandEnv *CommandEnv, writer io.Writer) error {

	return filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(filer.DirectoryEtcRemote), "", func(entry *filer_pb.Entry, isLast bool) error {
		if len(entry.Content) == 0 {
			fmt.Fprintf(writer, "skipping %s\n", entry.Name)
			return nil
		}
		if !strings.HasSuffix(entry.Name, filer.REMOTE_STORAGE_CONF_SUFFIX) {
			return nil
		}
		conf := &filer_pb.RemoteConf{}

		if err := proto.Unmarshal(entry.Content, conf); err != nil {
			return fmt.Errorf("unmarshal %s/%s: %v", filer.DirectoryEtcRemote, entry.Name, err)
		}

		conf.S3SecretKey = ""

		m := jsonpb.Marshaler{
			EmitDefaults: false,
			Indent:       "  ",
		}

		err := m.Marshal(writer, conf)
		fmt.Fprintln(writer)

		return err
	})

}

func (c *commandRemoteConfigure) deleteRemoteStorage(commandEnv *CommandEnv, writer io.Writer, storageName string) error {

	return commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:            filer.DirectoryEtcRemote,
			Name:                 storageName + filer.REMOTE_STORAGE_CONF_SUFFIX,
			IgnoreRecursiveError: false,
			IsDeleteData:         true,
			IsRecursive:          true,
			IsFromOtherCluster:   false,
			Signatures:           nil,
		}
		_, err := client.DeleteEntry(context.Background(), request)

		if err == nil {
			fmt.Fprintf(writer, "removed: %s\n", storageName)
		}

		return err

	})

}

func (c *commandRemoteConfigure) saveRemoteStorage(commandEnv *CommandEnv, writer io.Writer, conf *filer_pb.RemoteConf) error {

	data, err := proto.Marshal(conf)
	if err != nil {
		return err
	}

	if err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, filer.DirectoryEtcRemote, conf.Name+filer.REMOTE_STORAGE_CONF_SUFFIX, data)
	}); err != nil && err != filer_pb.ErrNotFound {
		return err
	}

	return nil

}
