
import os
from fastapi import Request, Response, status

class Authz(object):

    def __init__(self):
        cat_url = os.environ.get('PYICEBERG_CATALOG__DEFAULT__URI')


    def has_access(self, request: Request, response: Response, table_id: str):
        '''
        handle your own authorization logic here, make sure to return 
        status.HTTP_403_FORBIDDEN if user doesn't have access to table data
        '''
        user = request.session.get("user")
        if not user: #authentication is not enabled otherwise user would be in session
            return True
        if False:
            self.response.status_code = status.HTTP_403_FORBIDDEN
        return True
    
    def get_namespace_special_properties(self, namespace):
        pass

    def get_table_special_properties(self, table_id):
        pass
