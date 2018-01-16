import sys

from suds import metrics


class Timer:
    def start(self):
        pass

    def stop(self):
        pass

    def duration(self):
        return 0

    def __str__(self):
        return 'Timer disabled'


print('Applying Timer monkey patch...', file=sys.stderr)
metrics.Timer = Timer


from suds import builder


class Builder(builder.Builder):
    def build(self, name, shallow=False, set_type=None):
        if shallow:
            """ build a an object for the specified typename as defined in the schema """
            if isinstance(name, str):
                type = self.resolver.find(name)
                if type is None:
                    raise builder.TypeNotFound(name)
            else:
                type = name
            cls = type.name
            if type.mixed():
                data = builder.Factory.property(cls)
            else:
                data = builder.Factory.object(cls)
            resolved = type.resolve()
            md = data.__metadata__
            md.sxtype = resolved
            md.ordering = self.ordering(resolved)
            history = []
            self.add_attributes(data, resolved)
            for child, ancestry in type.children():
                if self.skip_child(child, ancestry):
                    continue
                self.process(data, child, history[:], shallow=shallow, set_type=set_type)
            return data
        else:
            return super().build(name)

    def process(self, data, type, history, shallow=False, set_type=None):
        if shallow:
            """ process the specified type then process its children """
            if type in history:
                return
            if type.enum():
                return
            history.append(type)
            resolved = type.resolve()
            value = None
            # adding AdWords "hack"
            if type.name.endswith('.Type'):
                setattr(data, type.name, set_type)
            else:
                setattr(data, type.name, value)

            if value is not None:
                data = value
            if not isinstance(data, list):
                self.add_attributes(data, resolved)
        else:
            return super().process(data, type, history)


print('Applying Builder monkey patch...', file=sys.stderr)
builder.Builder = Builder


from suds.mx import appender


class _ContentAppender:
    """
    Appender used to add content to marshalled objects.
    @ivar default: The default appender.
    @type default: L{Appender}
    @ivar appenders: A I{table} of appenders mapped by class.
    @type appenders: I{table}
    """

    def __init__(self, marshaller):
        """
        @param marshaller: A marshaller.
        @type marshaller: L{suds.mx.core.Core}
        """
        self.default = appender.PrimativeAppender(marshaller)
        self.appenders = {
            None: appender.NoneAppender(marshaller),
            appender.null: appender.NoneAppender(marshaller),
            appender.Property: appender.PropertyAppender(marshaller),
            appender.Object: appender.ObjectAppender(marshaller),
            appender.Element: appender.ElementAppender(marshaller),
            appender.Text: appender.TextAppender(marshaller),
            list: appender.ListAppender(marshaller),
            tuple: appender.ListAppender(marshaller),
            dict: appender.DictAppender(marshaller),
        }

    def append(self, parent, content):
        """
        Select an appender and append the content to parent.
        @param parent: A parent node.
        @type parent: L{Element}
        @param content: The content to append.
        @type content: L{Content}
        """
        for _class in type(content.value).mro():
            appender = self.appenders.get(_class)
            if appender:
                break
        else:
            appender = self.default

        appender.append(parent, content)


print('Applying ContentAppender monkey patch...', file=sys.stderr)
appender.ContentAppender = _ContentAppender


import suds
from suds import client


class Factory(client.Factory):
    def create(self, name, shallow=False, set_type=None):
        if shallow:
            """
            create a WSDL type by name
            @param name: The name of a type defined in the WSDL.
            @type name: str
            @return: The requested object.
            @rtype: L{Object}
            """
            timer = metrics.Timer()
            timer.start()
            type = self.resolver.find(name)
            if type is None:
                raise suds.TypeNotFound(name)
            if type.enum():
                result = suds.sudsobject.Factory.object(name)
                for e, a in type.children():
                    setattr(result, e.name, e.name)
            else:
                try:
                    result = self.builder.build(type, shallow, set_type)
                except Exception as e:
                    suds.log.error("create '%s' failed", name, exc_info=True)
                    raise suds.BuildError(name, e)
            timer.stop()
            metrics.log.debug('%s created: %s', name, timer)
            return result
        else:
            return super().create(name)


print('Applying Factory monkey patch...', file=sys.stderr)
client.Factory = Factory


from googleads import common


def _PackForSuds(obj, factory, packer=None):
    """Packs SOAP input into the format we want for suds.

    The main goal here is to pack dictionaries with an 'xsi_type' key into
    objects. This allows dictionary syntax to be used even with complex types
    extending other complex types. The contents of dictionaries and lists/tuples
    are recursively packed. Mutable types are copied - we don't mutate the input.

    Args:
        obj: A parameter for a SOAP request which will be packed. If this is
                a dictionary or list, the contents will recursively be packed. If this
                is not a dictionary or list, the contents will be recursively searched
                for instances of unpacked dictionaries or lists.
        factory: The suds.client.Factory object which can create instances of the
                classes generated from the WSDL.
        packer: An optional subclass of googleads.common.SudsPacker that provides
                customized packing logic.

    Returns:
        If the given obj was a dictionary that contained the 'xsi_type' key, this
        will be an instance of a class generated from the WSDL. Otherwise, this will
        be the same data type as the input obj was.
    """
    if packer:
        obj = packer.Pack(obj)

    if obj in ({}, None):
        # Force suds to serialize empty objects. There are legitimate use cases for
        # this, for example passing in an empty SearchCriteria object to a DFA
        # search method in order to select everything.
        return suds.null()
    elif isinstance(obj, dict):
        if 'xsi_type' in obj:
            try:
                new_obj = factory.create(obj['xsi_type'], shallow=True, set_type=obj['xsi_type'])
            except suds.TypeNotFound:
                new_obj = factory.create(':'.join(['ns0', obj['xsi_type']]), shallow=True, set_type=obj['xsi_type'])
            # Suds sends an empty XML element for enum types which are not set. None
            # of Google's Ads APIs will accept this. Initializing all of the fields in
            # a suds object to None will ensure that they don't get serialized at all
            # unless the user sets a value. User values explicitly set to None will be
            # packed into a suds.null() object.

            # this hack is now inside suds, using our monkey patch
            # for param, _ in new_obj:
            #     # Another problem is that the suds.mx.appender.ObjectAppender won't
            #     # serialize object types with no fields set, but both AdWords and DFP
            #     # rely on sending objects with just the xsi:type set. The below "if"
            #     # statement is an ugly hack that gets this to work in all(?) situations
            #     # by taking advantage of the fact that these classes generally all have
            #     # a type field. The only other option is to monkey patch ObjectAppender.
            #     if param.endswith('.Type'):
            #         setattr(new_obj, param, obj['xsi_type'])
            #     else:
            #         setattr(new_obj, param, None)
            for key in obj:
                if key == 'xsi_type': continue
                setattr(new_obj, key, _PackForSuds(obj[key], factory,
                                                   packer=packer))
        else:
            new_obj = {}
            for key in obj:
                new_obj[key] = _PackForSuds(obj[key], factory,
                                            packer=packer)
        return new_obj
    elif isinstance(obj, (list, tuple)):
        return [_PackForSuds(item, factory,
                             packer=packer) for item in obj]
    else:
        common._RecurseOverObject(obj, factory)
        return obj


print('Applying _PackForSuds monkey patch...', file=sys.stderr)
common._PackForSuds = _PackForSuds
