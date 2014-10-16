from furryninja import key_ref

__author__ = 'broken'


class PrimaryKeyMixin(object):
    @key_ref
    def kind(self):
        return self.key.kind

    @key_ref
    def revision(self):
        return '1'