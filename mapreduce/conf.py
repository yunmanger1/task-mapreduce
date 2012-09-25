import importlib
import os
import simplejson as json
from ConfigParser import SafeConfigParser


class BaseSettings(object):
    """
    Common logic for settings whether set by a module or by the user.
    """
    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)


class Settings(BaseSettings):

    def __init__(self, *setting_list, **kw):

        self.set_profile()

        settings_module = kw.pop('settings_module', None)
        settings_cfg = kw.pop('settings_cfg', None)
        setting_list = list(setting_list)
        if settings_module:
            setting_list.append(settings_module)
        if settings_cfg:
            setting_list.append(settings_cfg)

        for settings_file in setting_list:
            if settings_file.endswith('.cfg'):
                self.read_cfg(settings_file)
            elif settings_file.endswith('.json'):
                self.read_json(settings_file)
            else:
                self.read_module(settings_file)

    def set_profile(self):

          # Trying to import PROFILE from environment
        PROFILE = os.environ.get('SW_PROFILE', 'local')
        self.PROFILE = PROFILE

    def read_module(self, settings_module=None):
        if settings_module:
            try:
                mod = importlib.import_module(settings_module)
            except ImportError as e:
                raise ImportError(("Could not import settings '%s'"
                    " (Is it on sys.path?): %s") % (settings_module, e))

            for setting in dir(mod):
                setting_value = getattr(mod, setting)
                setattr(self, setting.upper(), setting_value)

    def read_cfg(self, cfg_file=None):

        if cfg_file:
            assert self.PROFILE, 'PROFILE variable is not set'
            cfg_file = cfg_file.format(profile=self.PROFILE)
            parser = SafeConfigParser()
            assert cfg_file in parser.read(cfg_file), \
                'Could not find: {}'.format(cfg_file)
            for name in parser.options('settings'):
                value = parser.get('settings', name)
                if value == 'true':
                    value = True
                elif value == 'false':
                    value = False
                elif value.isdigit():
                    value = int(value)
                setattr(self, name.upper(), value)

    def read_json(self, json_file=None):

        if json_file:
            assert self.PROFILE, 'PROFILE variable is not set'
            json_file = json_file.format(profile=self.PROFILE)
            json_data = json.loads(open(json_file, 'rb').read())

            assert isinstance(json_data, dict), \
                              'Is not dictionary : {}'.format(json_file)

            for key, value in json_data.iteritems():
                value = json_data.get(key, None)
                if value is not None:
                    setattr(self, key.upper(), value)
