import time
import traceback

from dcos.mesos import DCOSClient
from dcos import mesos
from shakedown import *
from utils import *


def app(id=1, instances=1):
    app_json = {
      "id": "",
      "instances":  1,
      "cmd": "for (( ; ; )); do sleep 100000000; done",
      "cpus": 0.01,
      "mem": 32,
      "disk": 0
    }
    if not str(id).startswith("/"):
        id = "/" + str(id)
    app_json['id'] = id
    app_json['instances'] = instances

    return app_json


def group(gcount=1, instances=1):
    id = "/2deep/group"
    group = {
        "id": id,
        "apps": []
    }

    for num in range(1, gcount + 1):
        app_json = app(id + "/" + str(num), instances)
        group['apps'].append(app_json)

    return group


def constraints(name, operator, value=None):
    constraints = [name, operator]
    if value is not None:
        constraints.append(value)
    return [constraints]


def unique_host_constraint():
    return constraints('hostname', 'UNIQUE')


def delete_all_apps():
    client = marathon.create_client()
    client.remove_group("/", True)


def time_deployment(test=""):
    client = marathon.create_client()
    start = time.time()
    deployment_count = 1
    while deployment_count > 0:
        # need protection when tearing down
        try:
            deployments = client.get_deployments()
            deployment_count = len(deployments)
            if deployment_count > 0:
                time.sleep(1)
        except:
            wait_for_service_endpoint('marathon-user')
            pass

    end = time.time()
    elapse = round(end - start, 3)
    return elapse


def delete_group(group="/2deep/group"):
    client = marathon.create_client()
    client.remove_group(group, True)


def delete_group_and_wait(group="test"):
    delete_group(group)
    time_deployment("undeploy")


def deployment_less_than_predicate(count=10):
    client = marathon.create_client()
    return len(client.get_deployments()) < count


def launch_apps(count=1, instances=1):
    client = marathon.create_client()
    for num in range(1, count + 1):
        # after 400 and every 50 check to see if we need to wait
        if num > 400 and num % 50 == 0:
            deployments = len(client.get_deployments())
            if deployments > 30:
                # wait for deployment count to be less than a sec
                wait_for(deployment_less_than_predicate)
                time.sleep(1)
        client.add_app(app(num, instances))


def launch_group(count=1, instances=1):
    client = marathon.create_client()
    client.create_group(group(count, instances))


def delete_all_apps_wait():
    delete_all_apps()
    time_deployment("undeploy")


def scale_test_apps(test_obj):
    if 'instance' in test_obj.style:
        instance_test_app(test_obj)
    if 'count' in test_obj.style:
        count_test_app(test_obj)
    if 'group' in test_obj.style:
        group_test_app(test_obj)


def get_current_tasks():

    try:
        return len(get_tasks())
    except Exception as e:
        print(e)
        return 0


def get_current_app_tasks(starting_tasks):
    return get_current_tasks() - starting_tasks


def count_test_app(test_obj):
    """
    Runs the `count` scale test for apps in marathon.   This is for apps and not pods.
    The count test is defined as X number of apps with Y number of instances.
    Y is commonly 1 instance and the test is scaling up to X number of applications.
    The details of how many apps and how many instances are defined in the test_obj.
    This test will make X number of HTTP requests against Marathon.

    :param test_obj: Is of type ScaleTest and defines the criteria for the test and logs the results and events of the test.
    """
    # make sure no apps currently
    delete_all_apps_wait2()

    test_obj.start = time.time()
    starting_tasks = get_current_tasks()

    # launch and
    launch_complete = True
    try:
        launch_apps2(test_obj)
    except:
        test_obj.add_event('Failure to fully launch')
        launch_complete = False
        wait_for_marathon_up(test_obj)
        pass

    # time to finish launch
    try:
        time_deployment2(test_obj, starting_tasks)
        launch_complete = True
    except Exception as e:
        assert False

    current_tasks = get_current_app_tasks(starting_tasks)
    test_obj.add_event('undeploying {} tasks'.format(current_tasks))

    # delete apps
    delete_all_apps_wait2(test_obj)

    assert launch_complete


def launch_apps2(test_obj):
    client = marathon.create_client()
    count = test_obj.count
    instances = test_obj.instance
    for num in range(1, count + 1):
        # after 400 and every 50 check to see if we need to wait
        if num > 400 and num % 50 == 0:
            deployments = len(client.get_deployments())
            if deployments > 30:
                # wait for deployment count to be less than a sec
                wait_for(deployment_less_than_predicate)
                time.sleep(1)
        try:
            client.add_app(app(num, instances))
        except Exception as e:
            time.sleep(1)
            test_obj.add_event('launch exception: {}'.format(str(e)))
            # either service not available or timeout of 10s
            wait_for_marathon_up(test_obj)


def instance_test_app(test_obj):
    """
    Runs the `instance` scale test for apps in marathon.   This is for apps and not pods.
    The instance test is defined as 1 app with X number of instances.
    the test is scaling up to X number of instances of an application.
    The details of how many instances are defined in the test_obj.
    This test will make 1 HTTP requests against Marathon.

    :param test_obj: Is of type ScaleTest and defines the criteria for the test and logs the results and events of the test.
    """

    # make sure no apps currently
    delete_all_apps_wait2()

    test_obj.start = time.time()
    starting_tasks = get_current_tasks()
    # launch apps
    launch_complete = True
    try:
        launch_apps2(test_obj)
    except:
        test_obj.failed('Failure to launched (but we still will wait for deploys)')
        launch_complete = False
        wait_for_marathon_up(test_obj)
        pass

    # time launch
    try:
        time_deployment2(test_obj, starting_tasks)
        launch_complete = True
    except Exception as e:
        assert False

    current_tasks = get_current_app_tasks(starting_tasks)
    test_obj.add_event('undeploying {} tasks'.format(current_tasks))

    # delete apps
    delete_all_apps_wait2(test_obj)

    assert launch_complete


def group_test_app(test_obj):
    """
    Runs the `group` scale test for apps in marathon.   This is for apps and not pods.
    The group test is defined as X number of apps with Y number of instances.
    Y number of instances is commonly 1.  The test is scaling up to X number of application and instances as submitted as 1 request.
    The details of how many instances are defined in the test_obj.
    This test will make 1 HTTP requests against Marathon.

    :param test_obj: Is of type ScaleTest and defines the criteria for the test and logs the results and events of the test.
    """
    # make sure no apps currently
    try:
        delete_all_apps_wait2()
    except:
        pass

    test_obj.start = time.time()
    starting_tasks = get_current_tasks()
    count = test_obj.count
    instances = test_obj.instance

    # launch apps
    launch_complete = True
    try:
        launch_group(count, instances)
    except:
        test_obj.failed('Failure to launched (but we still will wait for deploys)')
        launch_complete = False
        wait_for_marathon_up(test_obj)
        pass

    # time launch
    try:
        time_deployment2(test_obj, starting_tasks)
        launch_complete = True
    except Exception as e:
        assert False

    current_tasks = get_current_app_tasks(starting_tasks)
    test_obj.add_event('undeploying {} tasks'.format(current_tasks))

    # delete apps
    delete_all_apps_wait2(test_obj)

    assert launch_complete


def delete_all_apps_wait2(test_obj=None, msg='undeployment failure'):

    try:
        delete_all_apps()
    except Exception as e:
        if test_obj is not None:
            test_obj.add_event(msg)
        pass

    # some deletes (group test deletes commonly) timeout on remove_app
    # however it is a marathon internal issue on getting a timely response
    # all tested situations the remove did succeed
    try:
        undeployment_wait(test_obj)
    except Exception as e:
        msg = str(e)
        if test_obj is not None:
            test_obj.add_event(msg)
        assert False, msg


def undeployment_wait(test_obj=None):
    client = marathon.create_client()
    start = time.time()
    deployment_count = 1
    failure_count = 0
    while deployment_count > 0:
        # need protection when tearing down
        try:
            deployments = client.get_deployments()
            deployment_count = len(deployments)

            if deployment_count > 0:
                time.sleep(1)
                failure_count = 0
        except:
            failure_count += 1
            # consecutive failures great than x
            if failure_count > 10 and test_obj is not None:
                test_obj.failed('Too many failures waiting for undeploy')
                raise TestException()

            wait_for_marathon_up(test_obj)
            pass

    if test_obj is not None:
        test_obj.undeploy_complete(start)


def time_deployment2(test_obj, starting_tasks):
    client = marathon.create_client()
    target_tasks = starting_tasks + (test_obj.count * test_obj.instance)
    current_tasks = 0

    deployment_count = 1
    failure_count = 0
    while deployment_count > 0:
        # need protection when tearing down
        try:
            deployments = client.get_deployments()
            deployment_count = len(deployments)
            current_tasks = get_current_app_tasks(starting_tasks)

            if deployment_count > 0:
                time.sleep(1)
                failure_count = 0
        except:
            failure_count += 1
            # consecutive failures > x will fail test
            if failure_count > 10:
                test_obj.failed('Too many failures query for deployments')
                raise TestException()

            wait_for_marathon_up(test_obj)
            pass

    test_obj.successful()


def scale_apps(count=1, instances=1):
    test = "scaling apps: " + str(count) + " instances " + str(instances)

    start = time.time()
    launch_apps(count, instances)
    complete = False
    while not complete:
        try:
            time_deployment(test)
            complete = True
        except:
            time.sleep(2)
            pass

    launch_time = elapse_time(start, time.time())
    delete_all_apps_wait()
    return launch_time


def scale_groups(count=2):
    test = "group test count: " + str(instances)
    start = time.time()
    try:
        launch_group(count)
    except:
        # at high scale this will timeout but we still
        # want the deployment time
        pass

    time_deployment(test)
    launch_time = elapse_time(start, time.time())
    delete_group_and_wait("test")
    return launch_time


def elapse_time(start, end=None):
    if end is None:
        end = time.time()
    return round(end-start, 3)


def write_meta_data(test_metadata={}, filename='meta-data.json'):
    resources = available_resources()
    metadata = {
        'dcos-version': dcos_version(),
        'marathon-version': get_marathon_version(),
        'private-agents': len(get_private_agents()),
        'resources': {
            'cpus': resources.cpus,
            'memory': resources.mem
        }
    }

    metadata.update(test_metadata)
    with open(filename, 'w') as out:
        json.dump(metadata, out)


def get_marathon_version():
    client = marathon.create_client()
    about = client.get_about()
    return about.get("version")


def cluster_info(mom_name='marathon-user'):
    agents = get_private_agents()
    print("agents: {}".format(len(agents)))
    client = marathon.create_client()
    about = client.get_about()
    print("marathon version: {}".format(about.get("version")))
    # see if there is a MoM
    with marathon_on_marathon(mom_name):
        try:
            client = marathon.create_client()
            about = client.get_about()
            print("marathon MoM version: {}".format(about.get("version")))

        except Exception as e:
            print("Marathon MoM not present")


def get_mom_json(version='v1.3.6'):
    mom_json = get_resource("mom.json")
    docker_image = "mesosphere/marathon:{}".format(version)
    mom_json['container']['docker']['image'] = docker_image
    mom_json['labels']['DCOS_PACKAGE_VERSION'] = version
    return mom_json


def install_mom(version='v1.3.6'):
    # the docker tags start with v
    # however the marathon reports version without the v :(
    if not version.startswith('v'):
        version = 'v{}'.format(version)

    client = marathon.create_client()
    client.add_app(get_mom_json(version))
    print("Installing MoM: {}".format(version))
    deployment_wait()


def uninstall_mom():
    try:
        framework_id = get_service_framework_id('marathon-user')
        if framework_id is not None:
            print('uninstalling: {}'.format(framework_id))
            dcos_client = mesos.DCOSClient()
            dcos_client.shutdown_framework(framework_id)
            time.sleep(2)
    except:
        pass

    removed = False
    max_times = 10
    while not removed:
        try:
            max_times = max_times - 1
            client = marathon.create_client()
            client.remove_app('marathon-user')
            deployment_wait()
            time.sleep(2)
            removed = True
        except DCOSException:
            # remove_app throws DCOSException if it doesn't exist
            removed = True
            pass
        except Exception:
            # http or other exception and we retry
            traceback.print_exc()
            time.sleep(5)
            if max_time > 0:
                pass

    delete_zk_node('universe/marathon-user')


def wait_for_marathon_up(test_obj=None):
    if test_obj is None or 'root' in test_obj.mom:
        wait_for_service_endpoint('marathon')
    else:
        wait_for_service_endpoint('marathon-user')


def ensure_test_mom(test_obj):
    valid = ensure_mom_version(test_obj.mom_version)
    if not valid:
        test_obj.failed('Unable to install mom')

    return valid


def ensure_mom_version(version):
    if not is_mom_version(version):
        try:
            uninstall_mom()
            install_mom(version)
            wait_for_service_endpoint('marathon-user', 1200)
        except Exception as e:
            traceback.print_exc()
            return False
    return True


def is_mom_version(version):
    same_version = False
    max_times = 10
    check_complete = False
    while not check_complete:
        try:
            max_times == 1
            with marathon_on_marathon():
                client = marathon.create_client()
                about = client.get_about()
                same_version = version == about.get("version")
                check_complete = True
        except DCOSException:
            # if marathon doesn't exist yet
            pass
            return False
        except Exception as e:
            if max_times > 0:
                pass
                # this failure only happens at very high scale
                # it takes a lot of time to recover
                wait_for_service_endpoint('marathon-user', 600)
            else:
                return False
    return same_version


class DCOSScaleException(DCOSException):

    def __init__(self, message):
        self.message = message

    def message(self):
        return self.message

    def __str__(self):
        return self.message


class LaunchResults(object):

    def __init__(self, this_test):
        self.success = True
        self.avg_response_time = 0.0
        self.last_response_time = 0.0
        self.start = this_test.start
        self.current_test = this_test

    def __str__(self):
        return "launch  success: {} avg response time: {} last response time: {}".format(
            self.success,
            self.avg_response_time,
            self.last_response_time)

    def __repr__(self):
        return "launch  failure: {} avg response time: {} last response time: {}".format(
            self.success,
            self.avg_response_time,
            self.last_response_time)

    def current_response_time(self, response_time):
        if response_time > 0.0:
            self.last_response_time = response_time
            if self.avg_response_time == 0.0:
                self.avg_response_time = response_time
            else:
                self.avg_response_time = (self.avg_response_time + response_time)/2

    def complete(self):
        self.current_response_time(time.time())
        self.current_test.add_event('launch successful')

    def failure(self, message=''):
        self.success = False
        self.current_test.add_event('launch failed due to: {}'.format(message))


class DeployResults(object):

    def __init__(self, this_test):
        self.success = True
        self.avg_response_time = 0.0
        self.last_response_time = 0.0
        self.current_scale = 0
        self.target = this_test.target
        self.start = this_test.start

    def __str__(self):
        return "deploy  failure: {} avg response time: {} last response time: {} scale: {}".format(
            self.failure,
            self.avg_response_time,
            self.last_response_time,
            self.current_scale)

    def __repr__(self):
        return "deploy  failure: {} avg response time: {} last response time: {} scale: {}".format(
            self.success,
            self.avg_response_time,
            self.last_response_time,
            self.current_scale)

    def set_current_scale(self, task_count):
        # if task_count < current_scale exception
        if self.current_scale > task_count:
            raise DCOSScaleException('Scaling Failed:  Previous scale: {}, Current scale: {}'.format(
                self.current_scale,
                task_count))
        self.current_scale = task_count

    def current_response_time(self, response_time):
        if response_time > 0.0:
            self.last_response_time = response_time
            if self.avg_response_time == 0.0:
                self.avg_response_time = response_time
            else:
                self.avg_response_time = (self.avg_response_time + response_time)/2

class UnDeployResults(object):

    def __init__(self, this_test):
        self.success = True
        self.avg_response_time = 0.0
        self.last_response_time = 0.0
        self.start = this_test.start

    def __str__(self):
        return "undeploy  failure: {} avg response time: {} last response time: {}".format(
            self.success,
            self.avg_response_time,
            self.last_response_time)

    def __repr__(self):
        return "undeploy  failure: {} avg response time: {} last response time: {}".format(
            self.success,
            self.avg_response_time,
            self.last_response_time)


class ScaleTest(object):
    """ Defines a marathon scale test and collects the scale test data.
        A scale test has 3 phases of interest:  1) launching, 2) deploying and 3) undeploying

        `under_test` defines apps or pods
        `style` defines instance, count or group
            instance - is 1 app with X instances (makes 1 http launch call)
            count - is X apps with Y (often 1) instances each (makes an http launch for each X)
            group - is X apps in 1 http launch call

        All events are logged in the events array in order.
    """

    def __init__(self, name, mom, under_test, style, count, instance):
        # test style and criteria
        self.name = name
        self.under_test = under_test
        self.style = style
        self.instance = int(instance)
        self.count = int(count)
        self.start = time.time()
        self.mom = mom
        self.events = []
        self.target = int(instance) * int(count)

        # successful, failed, skipped
        # failure can happen in any of the test phases below
        self.status = 'running'
        self.test_time = None
        self.undeploy_time = None

        # results are in these objects
        self.launch_results = LaunchResults(self)
        self.deploy_results = DeployResults(self)
        self.undeploy_results = UnDeployResults(self)

    def __str__(self):
        return "test: {} status: {} time: {} events: {}".format(
            self.name,
            self.status,
            self.test_time,
            len(self.events))

    def __repr__(self):
        return "test: {} status: {} time: {} events: {}".format(
            self.name,
            self.status,
            self.test_time,
            len(self.events))

    def add_event(self, eventInfo):
        self.events.append('    event: {} (time in test: {})'.format(eventInfo, elapse_time(self.start)))

    def _status(self, status):
        """ end of scale test, however still may have events like undeploy_time
        this marks the end of the test time
        """
        self.status = status
        if 'successful' == status:
            self.test_time = elapse_time(self.start)
        else:
            self.test_time = 'x'

    def successful(self):
        self.add_event('successful')
        self._status('successful')

    def failed(self, reason="unknown"):
        self.add_event('failed: {}'.format(reason))
        self._status('failed')

    def skip(self, reason="unknown"):
        self.add_event('skipped: {}'.format(reason))
        self._status('skipped')

    def undeploy_complete(self, start):
        self.add_event('undeployment complete')
        self.undeploy_time = elapse_time(start)

    def log_events(self):
        for event in self.events:
            print(event)

    def log_stats(self):
        print('    *status*: {}, deploy: {}, undeploy: {}'.format(self.status, self.test_time, self.undeploy_time))


def start_test(name, marathons=None):
    """ test name example: test_mom1_apps_instances_1_100
    with list of marathons to test against.  If marathons are None, the root marathon is tested.
    """
    test = create_test_object(*name.split("_")[1:])
    if marathons is None:
        test.mom_version = 'root'
    else:
        test.mom_version = marathons[test.mom]
    return test


def create_test_object(marathon='root', under_test='apps', style='instances', num_apps=1, num_instances=1):
    test_name = 'test_{}_{}_{}_{}_{}'.format(marathon, under_test, style, num_apps, num_instances)
    test = ScaleTest(test_name, marathon, under_test, style, num_apps, num_instances)
    test.mom_version = marathon
    return test


def scaletest_resources(test_obj):
    return resources_needed(
        test_obj.instance,
        test_obj.count)


def outstanding_deployments():
    """ Provides a count of deployments still looking to land.
    """
    count = 0
    wait_for_marathon_up()
    client = marathon.create_client()
    queued_apps = client.get_queued_apps()
    for app in queued_apps:
        count = count + app['count']

    return count


def current_scale(app_id=None):
    """ Provides a count of tasks which are running on marathon.  The default
        app_id is None which provides a count of all tasks.
    """
    wait_for_marathon_up()
    client = marathon.create_client()
    tasks = client.get_tasks(app_id)
    return len(tasks)
