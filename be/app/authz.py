
import os
from fastapi import Request, Response, status

class Authz(object):

    def __init__(self, request: Request, response: Response):
        #self.token = os.environ.get('TOKEN')
        #self.user = request.session.get("user")
        self.response = response

    def has_access(self, iceberg_table: str):
        '''
        handle your own authorization logic here, make sure to return 
        status.HTTP_403_FORBIDDEN if user doesn't have access to table data
        '''
        if False:
            self.response.status_code = status.HTTP_403_FORBIDDEN
        return True
