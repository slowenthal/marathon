""" Test using enterprise marathon on marathon (MoM-EE). The individual steps
    to install MoM-EE are well documented here: 
    https://wiki.mesosphere.com/display/DCOS/MoM+1.4 
"""

import os

from common import *
from shakedown import *
from dcos import http

MOM_EE_NAME = 'marathon-user-ee'
MOM_EE_SERVICE_ACCOUNT = 'marathon_user_ee'
MOM_EE_SECRET_NAME = 'my-secret'

PRIVATE_KEY_FILE = 'private-key.pem'
PUBLIC_KEY_FILE = 'public-key.pem'

DEFAULT_MOM_IMAGES = {
    'MOM_EE_1.4': '1.4.1_1.9.7',
    'MOM_EE_1.3': '1.3.10_1.1.5'
}

def is_mom_ee_deployed():
    mom_ee_id = '/{}'.format(MOM_EE_NAME)
    client = marathon.create_client()
    apps = client.get_apps()
    return any(app['id'] == mom_ee_id for app in apps)


def remove_mom_ee():
    print('Removing {}...'.format(MOM_EE_NAME))
    if service_available_predicate(MOM_EE_NAME):
        with marathon_on_marathon(name=MOM_EE_NAME):
            delete_all_apps()

    client = marathon.create_client()
    client.remove_app(MOM_EE_NAME)
    deployment_wait()
    print('Successfully removed {}'.format(MOM_EE_NAME))


def mom_ee_image(version):
    image_name = 'MOM_EE_{}'.format(version)
    try:
        os.environ[image_name]
    except:
        default_image = DEFAULT_MOM_IMAGES[image_name]
        print('No environment override found for MoM-EE  v{}. Using default image {}'.format(version, default_image))
        return default_image


def assert_mom_ee(version, security_mode='permissive'):
    ensure_prerequisites_installed()
    ensure_service_account()
    ensure_permissions()
    ensure_secret(strict=True if security_mode == 'strict' else False)
    ensure_docker_credentials()

    # Deploy MoM-EE in permissive mode
    app_def_file = '{}/mom-ee-{}-{}.json'.format(fixture_dir(), security_mode, version)
    assert os.path.isfile(app_def_file), "Couldn't find appropriate MoM-EE definition: {}".format(app_def_file)

    image = mom_ee_image(version)
    print('Deploying {} definition with {} image'.format(app_def_file, image))

    app_def = get_resource(app_def_file)
    app_def['container']['docker']['image'] = 'mesosphere/marathon-dcos-ee:{}'.format(image)

    client = marathon.create_client()
    client.add_app(app_def)
    deployment_wait()
    wait_for_service_endpoint(MOM_EE_NAME)

@strict
@dcos_1_9
def test_mom_ee_strict_1_4():
    assert_mom_ee('1.4', 'strict')
    assert simple_sleep_app()

@dcos_1_9
def test_mom_ee_permissive_1_4():
    assert_mom_ee('1.4', 'permissive')
    assert simple_sleep_app()

@dcos_1_9
def test_mom_ee_disabled_1_4():
    assert_mom_ee('1.4', 'disabled')
    assert simple_sleep_app()

@strict
@dcos_1_9
def test_mom_ee_strict_1_3():
    assert_mom_ee('1.3', 'strict')
    assert simple_sleep_app()

@dcos_1_9
def test_mom_ee_permissive_1_3():
    assert_mom_ee('1.3', 'permissive')
    assert simple_sleep_app()

@dcos_1_9
def test_mom_ee_disabled_1_3():
    assert_mom_ee('1.3', 'disabled')
    assert simple_sleep_app()


def simple_sleep_app():
    # Deploy a simple sleep app in the MoM-EE
    with marathon_on_marathon(name=MOM_EE_NAME):
        client = marathon.create_client()
        
        app_id = uuid.uuid4().hex
        app_def = app(app_id)
        client.add_app(app_def)
        deployment_wait()

        tasks = get_service_task(MOM_EE_NAME, app_id)
        print('MoM-EE tasks: {}'.format(tasks))
        return tasks is not None


def ensure_prerequisites_installed():
    if not is_enterprise_cli_package_installed():
        install_enterprise_cli_package()
    assert is_enterprise_cli_package_installed() == True


def ensure_service_account():
    if has_service_account(MOM_EE_SERVICE_ACCOUNT):
        delete_service_account(MOM_EE_SERVICE_ACCOUNT)
    create_service_account(MOM_EE_SERVICE_ACCOUNT, PRIVATE_KEY_FILE, PUBLIC_KEY_FILE)
    assert has_service_account(MOM_EE_SERVICE_ACCOUNT)


def ensure_permissions():
    set_service_account_permissions(MOM_EE_SERVICE_ACCOUNT)

    url = '{}acs/api/v1/acls/dcos:superuser/users/{}'.format(dcos_url(), MOM_EE_SERVICE_ACCOUNT)
    req = http.get(url)
    assert req.json()['array'][0]['url'] == '/acs/api/v1/acls/dcos:superuser/users/{}/full'.format(MOM_EE_SERVICE_ACCOUNT), "Service account permissions couldn't be set"


def ensure_secret(strict=False):
    if has_secret(MOM_EE_SECRET_NAME):
        delete_secret(MOM_EE_SECRET_NAME)
    create_secret(MOM_EE_SECRET_NAME, MOM_EE_SERVICE_ACCOUNT, strict)
    assert has_secret(MOM_EE_SECRET_NAME)


def ensure_docker_credentials():
    create_and_copy_docker_credentials_file(get_private_agents())


def setup_function(function):
    if is_mom_ee_deployed():
        remove_mom_ee()
    assert not is_mom_ee_deployed()


def teardown_module(module):
    remove_mom_ee()
    delete_service_account(MOM_EE_SERVICE_ACCOUNT)
    delete_secret(MOM_EE_SECRET_NAME)

