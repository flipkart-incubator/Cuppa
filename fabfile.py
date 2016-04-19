from fabric.api import task, local, lcd

@task
def make_knnaas():
    local('rm -rf knnaas/worker/build')
    local('mkdir -p knnaas/worker/build')
    with lcd('knnaas/worker/build'):
        local('ls')
        local('pwd')
        local('cmake ..')
        local('make')

@task
def make_caffe():
    with lcd('build'):
        local('git clone https://github.com/nikhilketkar/caffe.git')
    with lcd('build/caffe'):
        local('rm -rf build')
        local('mkdir build')
    with lcd('build/caffe/build'):
        local('cmake ..')
        local('make')
        local('make pycaffe')

@task
def download_example_models():
    local('mkdir -p caffe_models')
    #KevinNet - https://github.com/kevinlin311tw/caffe-cvprw15
    local('wget -O ./caffe_models/KevinNet_CIFAR10_48_blob "https://www.dropbox.com/s/1om7xa8mz93wkzh/KevinNet_CIFAR10_48.caffemodel?dl=1"')
    local('wget -O ./caffe_models/KevinNet_CIFAR10_48_proto "https://raw.githubusercontent.com/kevinlin311tw/caffe-cvprw15/master/examples/cvprw15-cifar10/KevinNet_CIFAR10_48_deploy.prototxt"')

@task
def setup():
    local('rm -rf build')
    local('mkdir build')
    make_caffe()
    make_knnaas()
    local('mkdir -p caffe_models')
    download_example_models()
    # add locale settings to bash_profile for ipython notebook to work

@task
def make():
    make_knnaas()

@task
def start_redis():
    local('sudo /etc/init.d/redis-server start')

@task
def stop_redis():
    local('sudo /etc/init.d/redis-server stop')

@task
def start_router():
    local("LOG_CFG=test/router_log_config.yaml gunicorn --daemon 'router.router_app:loader()' --error-logfile gunicorn.log --access-logfile gunicorn_access.log")

@task
def stop_router():
    local("pkill -f 'router.router_app'")

@task
def start_caas():
    local('python -m caas.worker.worker start conf/caas_config.yaml 0')

@task
def stop_caas():
    local('python -m caas.worker.worker stop conf/caas_config.yaml 0')

@task
def start_knnaas():
    local('python -m knnaas.worker.server start conf/knnaas_config.yaml 0')

@task
def stop_knnaas():
    local('python -m knnaas.worker.server stop conf/knnaas_config.yaml 0')

@task
def start_ipynb():
    with lcd('examples'):
        local('LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 ipython notebook --ip="*"')
