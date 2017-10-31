package main

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"path/filepath"
	"strings"
	dkvolume "github.com/docker/go-plugins-helpers/volume"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/cephfs"
	"os/exec"
	"os"
	"io"
	"bufio"
)

type volume struct {
	name string
	cephfs_root string
	connections int
}

type cephfsDriver struct {
	name	string
	cluster string
	root	string
	config	string
	volumes	map[string]*volume
	m	*sync.Mutex
}

func newCephfsDriver(pluginName,cluster,rootBase,config string) cephfsDriver {
	mountDir := filepath.Join(rootBase,pluginName)
	log.Printf("INFO: newCephRBDVolumeDriver: setting base mount dir=%s", mountDir)
	d := cephfsDriver {
		name:	pluginName,
		cluster: cluster,
		root:	mountDir,
		config:	config,
		volumes:	map[string]*volume{},
		m:	&sync.Mutex{},
	}
	d.connect()
	return d
}

func (d *cephfsDriver) connect(){
	log.Printf("INFO: new connecting to ceph")
	d.m.Lock()
	defer d.m.Unlock()

	var err error
	cephConn,err := rados.NewConn()
	if err != nil {
		log.Panicf("ERROR: Unable to create ceph connection to cluster=%s",d.cluster)
	}
	if d.config == ""{
		err = cephConn.ReadDefaultConfigFile()
	}else {
		err = cephConn.ReadConfigFile(d.config)
	}
	if err != nil {
		log.Panicf("ERROR: Unable to read ceph config:%s",d.config)
	}
	err = cephConn.Connect()
	if err != nil {
		log.Panicf("ERROR: Unable to connect to ceph:%s",err)
	}
}

func (d *cephfsDriver) DirExists(dirname string) (*cephfs.MountInfo,bool,error) {
	log.Printf("INFO:createMount %s",dirname)
	mount, err := cephfs.CreateMount()
	if err!= nil {
		log.Printf("ERROR: create mount: %s",err)
		return nil,false,err
	}
	err = mount.ReadDefaultConfigFile()
	if err != nil {
		log.Printf("ERROR: ReadDefaultConfigFile: %s",err)
		return nil,false,err
	}
	mount.Mount()
	mount.ChangeDir(dirname)
	dir := mount.CurrentDir()
	if dir == dirname {
		return mount,true,nil
	}
	return mount,false,nil
}

func (d cephfsDriver) Create(r  *dkvolume.CreateRequest) error {
	log.Printf("INFO: Create %v",r)
	d.m.Lock()
	defer d.m.Unlock()

	return d.Createdir(r)
}

func (d *cephfsDriver) Createdir(r *dkvolume.CreateRequest) error {
	log.Printf("INFO: Create dir %v",r)
	dirname,err := d.parseName(r.Name)
	if err != nil {
		log.Printf("ERROR: parsing volume: %s",err)
		return err
	}
	mount := d.mountpoint(dirname)
	if _,found := d.volumes[mount];found{
		log.Printf("INFO: Volume is already mounted: " +mount )
		return nil
	}
	m,exists,err := d.DirExists(dirname)
	if err != nil {
		log.Printf("Warn: checking for dirname: %s",err)
		return err
	}
	if !exists{
		err = m.MakeDir(dirname,0755)
		if err != nil {
			errString := fmt.Sprint("Unable to create dirname(%s):%s",dirname,err)
			log.Printf("ERROR: "+errString)
			return err
		}
	}
	return nil

}

func (d cephfsDriver) Mount(r *dkvolume.MountRequest) (*dkvolume.MountResponse, error) {
	log.Printf("INFO: Mount %s",r.Name)
	d.m.Lock()
	defer d.m.Unlock()

	dirname,err := d.parseName(r.Name)
	if err != nil {
		log.Printf("ERROR: parsing volume: %s",err)
                return nil, err
	}
	mountpoint := d.mountpoint(dirname)
	fi,err := os.Lstat(mountpoint)
	if os.IsNotExist(err){
		if err1 := os.MkdirAll(mountpoint,0755);err1 !=nil {
                        return nil, err
		}
	} else if err != nil {
                return nil, err
	}
	if fi != nil && !fi.IsDir(){
                return nil, err
	}
	 m,exists,err := d.DirExists(dirname)
        if err != nil {
                log.Printf("ERROR: checking dirname:%s",dirname)
                return nil, err
        }
	if !exists{
                err = m.MakeDir(dirname,0755)
                if err != nil {
                        errString := fmt.Sprint("Unable to create dirname(%s):%s",dirname,err)
                        log.Printf("ERROR: "+errString)
                        return nil, err
                }
        }
	err =  d.mountvolume(dirname,mountpoint)
	if err != nil {
                return &dkvolume.MountResponse{Mountpoint: mountpoint}, err
                
	}
	vol,ok := d.volumes[dirname]
	if ok && vol.connections > 0 {
		vol.connections++
	}else {
		d.volumes[dirname] = &volume{name:strings.Trim(dirname,"/"),cephfs_root:"/"+strings.Trim(dirname,"/"),connections:1}
	}
        return &dkvolume.MountResponse{Mountpoint: mountpoint}, nil
}

func (d cephfsDriver) Unmount(r *dkvolume.UnmountRequest) error {
	log.Printf("INFO: Unmount %s",r.Name)
	d.m.Lock()
	defer d.m.Unlock()

	dirname,err := d.parseName(r.Name)
	if err != nil {
		log.Printf("ERROR: parsing volume: %s",err)
		return err
	}
	mountpoint := d.mountpoint(dirname)
	if volume,ok := d.volumes[dirname];ok && volume.connections >=1 {
		if err = d.unmountvolume(mountpoint);err!= nil {
			return err
		}
		volume.connections--
		log.Printf("INFO: volume.connections %d",volume.connections)
	}else{
		return errors.New("Unable to find volume mounted")
	}
	return nil
}

func (d cephfsDriver) Remove(r *dkvolume.RemoveRequest) error {
	log.Printf("INFO: Remove %s",r.Name)
	d.m.Lock()
	defer d.m.Unlock()

	dirname,err := d.parseName(r.Name)
	if err != nil {
		log.Printf("ERROR: parsing volume: %s",err)
		return err
	}
	mountpoint := d.mountpoint(dirname)
	cmd:=  fmt.Sprintf("rm -rf %s/%s",mountpoint,dirname)
	cmd1:=  fmt.Sprintf("rm -rf %s/*",mountpoint)
	log.Printf("INFO: rm cmd: %s",cmd)
	if volume,ok := d.volumes[dirname];ok {
		if volume.connections ==1 {
			if _,err = sh(cmd1); err != nil {
				return err
			}
		}else if volume.connections == 0 {
			if err = d.mountvolume("/",mountpoint);err != nil {
				return err
			}
			if _,err = sh(cmd); err != nil {
                                return err
			}
		}
		if err = d.unmountvolume(mountpoint);err != nil {
			return err
		}
		delete(d.volumes,dirname)
	}
	return nil
}

func (d cephfsDriver) List() (*dkvolume.ListResponse, error) {
	vols := make([]*dkvolume.Volume,0,len(d.volumes))
	for k,v := range d.volumes{
		mountpoint := d.mountpoint(k)
		vols = append(vols,&dkvolume.Volume{
			Name: v.name,
			Mountpoint: mountpoint,
			})
	}
	log.Printf("INFO: List request => %s",vols)
	return &dkvolume.ListResponse{Volumes:vols}, nil
}


func (d cephfsDriver) Get(r *dkvolume.GetRequest) (*dkvolume.GetResponse, error)  {
	dirname,err := d.parseName(r.Name)
	if err != nil {
		log.Printf("ERROR: parsing volume: %s",err)
                return nil,err
	}
	_,exists,err := d.DirExists(dirname)
	if err != nil {
		log.Printf("ERROR: checking dirname:%s",dirname)
                return nil,err
	}
	if !exists {
		log.Printf("WARN: dir %s does not exists",dirname)
		delete(d.volumes,dirname)
                return nil,err
	}
	mountpoint := d.mountpoint(dirname)
	log.Printf("INFO: Get request(%s)",dirname)
        return &dkvolume.GetResponse{Volume:&dkvolume.Volume{Name:dirname,Mountpoint:mountpoint}}, err
}

func (d cephfsDriver)Capabilities() *dkvolume.CapabilitiesResponse {
	var res *dkvolume.CapabilitiesResponse
        res.Capabilities = dkvolume.Capability{Scope: "global"}
	return res
}



func (d cephfsDriver) Path(r *dkvolume.PathRequest) (*dkvolume.PathResponse, error) {
	dirname,err := d.parseName(r.Name)
	if err != nil {
		log.Printf("ERROR: parsing volume: %s",err)
		return nil, err
	}
	return &dkvolume.PathResponse{Mountpoint: d.mountpoint(dirname)}, nil
}

func (d *cephfsDriver) mountvolume(name,target string) error{
	log.Printf("INFO: mountvolume %s,%s ",name,target)
	mds_hostname,err := getMds()
	if err != nil {
		return err
	}
	key,err := getKey()
	if err != nil {
		return err
	}
        cmd := "ceph-fuse -k /etc/ceph/ceph.client.admin.keyring -m"+mds_hostname+":6789:"+name+" "+target+""
        log.Printf("INFO: mountcmd %s",cmd)
        log.Printf("INFO: key %s",key)
	_,err = sh(cmd)
	return err
}

func (d *cephfsDriver) unmountvolume(target string) error{
	log.Printf("INFO: unmountvolume %s",target)
	cmd := "umount "+target
	log.Printf("INFO unmount cmd: %s",cmd)
	_,err := sh(cmd)
	return err
}

func (d *cephfsDriver) mountpoint(name string) string{
	return filepath.Join(d.root,name)
}

func (d *cephfsDriver) parseName(fullname string)(string,error){
	fullname = strings.TrimSpace(fullname)
	fullname = strings.TrimLeft(fullname,"/")
	if len(strings.TrimSpace(fullname))== 0{
		return "",errors.New("Unable to parse dir name")
	}
	fullname = "/"+fullname
	return strings.TrimSpace(fullname),nil
}


func  getMds() (string,error) {
	cmd := "ceph mds stat | awk -F '=' '{print $2}'"
	mds_hostname,err := sh(cmd)
	return mds_hostname,err
}

func sh(name string)(string,error){
	cmd :=  exec.Command("sh","-c",name)
	out,err := cmd.Output()
	return strings.Trim(string(out),"\n"),err
}
func getKey() (string,error) {
        f, err := os.Open("/etc/ceph/ceph.client.admin.keyring")
        if err != nil {
                return "",err
        }
        defer f.Close()
        var line1 string
        rd := bufio.NewReader(f)
        for {
                line, err := rd.ReadString('\n')
                if err != nil || io.EOF == err {
                    break
                }
                if strings.Contains(line,"key"){
                        line = strings.TrimSpace(line)
                        line = strings.TrimLeft(line,"key = ")
                        line1 = line
                }
        }
	log.Printf("INFO: key: %s",line1)
        return line1,nil
}
