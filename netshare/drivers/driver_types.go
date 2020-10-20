package drivers

type DriverType int

const (
	CIFS DriverType = iota
	NFS
	EFS
	CEPH
	SEAWEEDFS
)

var driverTypes = []string{
	"cifs",
	"nfs",
	"efs",
	"ceph",
	"seaweedfs",
}

func (dt DriverType) String() string {
	return driverTypes[dt]
}
