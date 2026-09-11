import json,sys,os,subprocess,hashlib
from pathlib import Path
for edit in json.load(open(sys.argv[1])):
    if 'delete' in edit:
        Path(edit['delete']).unlink()
    elif 'rename' in edit:
        old,new = map(Path, edit['rename'])
        new.parent.mkdir(parents=True,exist_ok=True)
        old.rename(new)
    elif 'patch' in edit:
        subprocess.run(['git','apply','-'],input=edit['patch'].encode(),check=True)
    else:
        target=Path(edit['file'])
        target.parent.mkdir(parents=True,exist_ok=True)
        if target.is_symlink(): target.unlink()
        if edit['mode']=='120000':
            target.symlink_to(edit['content'])
        else:
            target.write_text(edit['content'])
            target.chmod(int(edit['mode'],8)&0o777)
    if 'sha' in edit:
        target=Path(edit['target'])
        content=os.readlink(target).encode() if target.is_symlink() else target.read_bytes()
        assert hashlib.sha256(content).hexdigest()==edit['sha'], target
