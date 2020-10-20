package drivers

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/docker/go-plugins-helpers/volume"
	log "github.com/sirupsen/logrus"
)

const (
	SeaweedfsOptions = "seaweedfsopts"
)

type seaweedfsDriver struct {
	volumeDriver
	filer         string
	filerPath     string
	filerPort     int
	seaweedfsOpts map[string]string
	// check on stdout ??
	runningMounts map[string]int
}

func NewSeaweedfsDriver(root string, filer string, filerPort int, filerPath string, seaweedfsOpts string, mounts *MountManager) seaweedfsDriver {
	d := seaweedfsDriver{
		volumeDriver:  newVolumeDriver(root, mounts),
		filer:         filer,
		filerPath:     filerPath,
		filerPort:     filerPort,
		seaweedfsOpts: map[string]string{},
		runningMounts: map[string]int{},
	}
	if len(seaweedfsOpts) > 0 {
		d.seaweedfsOpts[SeaweedfsOptions] = seaweedfsOpts
	}

	return d
}

func (n seaweedfsDriver) Mount(r *volume.MountRequest) (*volume.MountResponse, error) {
	log.Debugf("Entering Mount: %v", r)
	n.m.Lock()
	defer n.m.Unlock()
	hostdir := mountpoint(n.root, r.Name)
	source := r.Name
	if n.mountm.HasMount(r.Name) && n.mountm.Count(r.Name) > 0 {
		log.Infof("Using existing Seaweedfs volume mount: %s", hostdir)
		n.mountm.Increment(r.Name)
		return &volume.MountResponse{Mountpoint: hostdir}, nil
	}

	log.Infof("Mounting Seaweedfs volume %s on %s", source, hostdir)
	if err := createDest(hostdir); err != nil {
		return nil, err
	}

	if err := n.mountVolume(r.Name, source, hostdir); err != nil {
		return nil, err
	}
	n.mountm.Add(r.Name, hostdir)
	return &volume.MountResponse{Mountpoint: hostdir}, nil
}

func (n seaweedfsDriver) Unmount(r *volume.UnmountRequest) error {
	log.Debugf("Entering Unmount: %v", r)

	n.m.Lock()
	defer n.m.Unlock()
	hostdir := mountpoint(n.root, r.Name)

	if n.mountm.HasMount(r.Name) {
		if n.mountm.Count(r.Name) > 1 {
			log.Printf("Skipping unmount for %s - in use by other containers", r.Name)
			n.mountm.Decrement(r.Name)
			return nil
		}
		n.mountm.Decrement(r.Name)
	}

	log.Infof("Unmounting volume name %s from %s", r.Name, hostdir)

	if err := run(fmt.Sprintf("umount %s", hostdir)); err != nil {
		return err
	}

	n.mountm.DeleteIfNotManaged(r.Name)

	if err := os.RemoveAll(hostdir); err != nil {
		return err
	}

	return nil
}

func (n seaweedfsDriver) parseSource(source string) (string, int, string) {
	// source format is [filer]/filerPath
	// filer may be omitted, use default filer in this case
	sourceParts := strings.Split(source, "/")
	if sourceParts[0] == "" {
		return "", 0, strings.Join(sourceParts, "/")
	}
	hostPort := strings.Split(sourceParts[0], ":")
	path := "/" + strings.Join(sourceParts[1:], "/")
	if len(hostPort) > 1 {
		val, err := strconv.Atoi(hostPort[1])
		if err != nil {
			log.Debugf("Invalid port number %s %s", hostPort[1], err)
			return hostPort[0], 0, path
		}
		return hostPort[0], val, path
	}

	return sourceParts[0], 0, path
}

func (n seaweedfsDriver) mountVolume(name, source, dest string) error {
	options := merge(n.mountm.GetOptions(name), n.seaweedfsOpts)

	opts := []string{}
	if val, ok := options[SeaweedfsOptions]; ok {
		log.Debugf("opts = %s", val)
		opts = strings.Split(val, ",")
	}

	filer, filerPort, filerPath := n.parseSource(source)

	if filerPort == 0 {
		filerPort = n.filerPort
	}
	if filer == "" {
		filer = n.filer
	}
	args := []string{
		"mount",
		fmt.Sprintf("-dir=%s", dest),
		fmt.Sprintf("-filer=%s:%d", filer, filerPort),
		fmt.Sprintf("-filer.path=%s", filerPath),
	}
	args = append(args, opts...)

	return n.runMount(name, args...)
}

func (n seaweedfsDriver) runMount(name string, args ...string) error {
	cmd := exec.Command(
		"weed",
		args...,
	)
	log.Debugf("exec: %s\n", cmd.String())
	if err := cmd.Start(); err != nil {
		log.Errorf("Error while starting seaweedfs mount %v\n", err)
		return err
	}

	// os.Process.Signal(0) => if err == nil => alive
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		log.Errorf("Process is dead %s (signal %v)\n", name, err)
		return fmt.Errorf("Process is dead for %s", name)
	}
	if err := cmd.Process.Release(); err != nil {
		log.Errorf("Error while releasing process for %s\n", name)
		return err
	}

	n.runningMounts[name] = cmd.Process.Pid

	return nil
}

func (n seaweedfsDriver) mountOptions(src map[string]string) map[string]string {

	if len(n.seaweedfsOpts) == 0 && len(src) == 0 {
		return EmptyMap
	}

	dst := map[string]string{}
	for k, v := range n.seaweedfsOpts {
		dst[k] = v
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
