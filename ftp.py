import pycurl
from io import BytesIO

def dirlist(u):
    # lets create a buffer in which we will write the output
    buffer = BytesIO()
    
    # lets create a pycurl object
    c = pycurl.Curl()

    # lets specify the details of FTP server
    # c.setopt(pycurl.URL, r'ftp://ftp.ncbi.nih.gov/refseq/release/')
    c.setopt(pycurl.URL, u)

    # lets assign this buffer to pycurl object
    c.setopt(pycurl.WRITEFUNCTION, buffer.write)

    # lets perform the LIST operation
    try:
        c.perform()
    except:
        code = c.getinfo(pycurl.HTTP_CODE)
        print('ERROR: cURL HTTP_CODE:', code)
    c.close()

    # lets get the buffer in a string
    body = buffer.getvalue()
    print(body)
    return body  # Returns Bytes!!!


def file(u, o):
    with open(o, 'wb') as f:
        c = pycurl.Curl()
        c.setopt(c.URL, u)
        c.setopt(c.WRITEDATA, f)
        c.setopt(c.NOPROGRESS, 1)  # Show (0) or not show (1) the progress
        print('INFO: Downloading file from {u} to {o}'.format(o=o, u=u))
        try:
            c.perform()
        except:
            code = c.getinfo(pycurl.HTTP_CODE)
            print('ERROR: cURL HTTP_CODE:', code)
        c.close()