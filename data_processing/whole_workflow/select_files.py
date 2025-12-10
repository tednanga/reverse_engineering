

class FileSelect:
    
    def __init__(self, list_names):
        self.list_names = [n.split('.')[0] for n in list_names]
    
    def __getitem__(self, idx):
        return self.list_names[idx]
        
    def __len__(self):
        return len(self.list_names)
        
    
    def __call__(self, name):
        name = name.split('.')[0]
        for f in self.list_names:
            if f in name:
                return f + '.xlsx'
        