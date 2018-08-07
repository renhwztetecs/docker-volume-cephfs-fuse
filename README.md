# docker-volume-cephfs-fuse
Docker volume driver plugin for CephFS

Dependent: <br>
github.com/docker/go-plugins-helpers/volume <br>
github.com/ceph/go-ceph/rados <br>
github.com/ceph/go-ceph/cephfs <br>
/usr/local/go/pkg/src/github.com/ceph/go-ceph is download


Build: <br>
go build -o xxx driver.go main.go <br>
`example:
go build -o docker-volume-cephfs driver.go main.go`

Run: <br>
./usr/local/go/bin/docker-volume-cephfs <br>
docker run --name vu1 -d -P --volume-driver=root -v test:/root/mycephfs -it centos_7.1503 /bin/bash <br>

