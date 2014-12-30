import pwd
import os
import sys
import itertools
import subprocess

def __fix_perms(userinfo):
    """ If _MEIPASS is defined we are executing in a pyinstaller environment
    In that case we need to recursively chown that directory to our target user before continuing.
    """
    try:
        # we can't check to see if this exists since it is set weirdly
        target = sys._MEIPASS
    except:
        return

    # try to use the chown command, if that fails then we fallback to python
    try:
        with open('/dev/null', 'wb') as devnull:
            subprocess.check_call(['chown', '-R', '%s:%s' % (userinfo.pw_uid, userinfo.pw_gid), target], stdout=devnull, stderr=devnull)
    except (OSError, subprocess.CalledProcessError):
        os.chown(target, userinfo.pw_uid, userinfo.pw_gid)
        for root, dirs, files in os.walk(target):
            for f in itertools.chain(dirs, files):
                os.chown(os.path.join(root, f), userinfo.pw_uid, userinfo.pw_gid)

def setuser(user_or_pid):
    try:
        try:
            userinfo = pwd.getpwuid(int(user_or_pid))
        except ValueError:
            userinfo = pwd.getpwnam(user_or_pid)

        __fix_perms(userinfo)
        os.setgid(userinfo.pw_gid)
        os.setuid(userinfo.pw_uid)
        return True
    except KeyError as e:
        print(str(e))
        return False
