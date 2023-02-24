import requests
import pandas
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry
from intake.catalog.utils import reload_on_change


RAW_URL = "https://raw.githubusercontent.com"
GH_URL = "https://www.github.com"
REPO = "fivethirtyeight/data"
GH_API = "https://api.github.com"

def get_projects(search_string: str = None, gittree: dict = None, render: bool = True) -> pandas.DataFrame:
    """Given a search string, look for fivethirtyeight projects that include that word. If NULL return all"""

    if gittree == None:

        # Get the default branch
        response = requests.get(f"{GH_API}/repos/{REPO}")
        response.raise_for_status()
        
        repo = response.json()
        
        # Get the sha for the default branch
        response = requests.get(f"{GH_API}/repos/{REPO}/branches/{repo['default_branch']}")
        response.raise_for_status()
        
        branch = response.json()

        # get the full tree
        response = requests.get(f"{GH_API}/repos/{REPO}/git/trees/{branch['commit']['sha']}?recursive=1")
        response.raise_for_status()
        
        gittree = response.json()
    
    sha = gittree['sha']
    # Projects are just top-level directories, so only include directories (type=tree) and without a / (not a subdirectory)
    project_list = [x for x in gittree['tree'] if x['type'] == 'tree' and x['path'].find('/') < 0]

    if search_string != None:
        project_list = [x for x in project_list if x['path'].find(search_string) >=0]
    
    records = []
    for dataset in project_list:
        description = ""
        url = f"{GH_URL}/{REPO}/tree/{sha}/{dataset['path']}"
        
        # First get the readme file in the matching project directory
        readme = [x for x in gittree['tree'] if x['type'] == 'blob' and x['path'].startswith(dataset['path']) and x['path'].lower().endswith('readme.md')]
        
        readme_url = f"{RAW_URL}/{REPO}/{sha}/{readme[0]['path']}"
        response = requests.get(readme_url)
        response.raise_for_status()

        # Pull out the title first (look for a line starting with # 
        description = response.text
                
        records.append({
            "Name": dataset['path'],
            "Description": description,
            "URL": url
        })
    
    return pandas.DataFrame(records)
    
class Five38Catalog(Catalog):
    name = "fivethirtyeight_projects"
    
    def __init__(self, query: str = "", **kwargs):
        self._search_string = query
        super().__init__(**kwargs)

    def search(self, text : str = None, **_) -> "Five38Catalog":
        return Five38Catalog(text)

    # total hack because the Catalog base class expects _user_parameters in entries, which Catalogs don't have
    @reload_on_change
    def _get_entry(self, name):
        entry = self._entries[name]
        entry._catalog = self
        entry._pmode = self.pmode

        up_names = set((up["name"] if isinstance(up, dict) else up.name)
                        for up in entry.user_parameters)
        ups = [up for name, up in self.user_parameters.items() if name not in up_names]
        entry._user_parameters = ups + (entry.user_parameters or [])
        return entry()

    def _load(self):
        self._entries = {}
        
        # Get the default branch
        response = requests.get(f"{GH_API}/repos/{REPO}")
        response.raise_for_status()
        
        repo = response.json()
        
        # Get the sha for the default branch
        response = requests.get(f"{GH_API}/repos/{REPO}/branches/{repo['default_branch']}")
        response.raise_for_status()
        
        branch = response.json()

        # get the full tree
        response = requests.get(f"{GH_API}/repos/{REPO}/git/trees/{branch['commit']['sha']}?recursive=1")
        response.raise_for_status()
        
        tree = response.json()
        
        projects = get_projects(self._search_string,tree)

        sha = tree['sha']

        for dataset in projects.to_dict(orient="records"):
            # We'll need to create a Catalog for each dataset
            project_name = dataset['Name']
            sub_catalog = Five38SubCatalog(
                tree=tree,
                name=project_name,
                description=dataset["Description"],
                ttl=99999999999
            )
            self._entries[project_name.replace('-','_')] = sub_catalog

            
    @reload_on_change
    def walk(self, sofar=None, prefix=None, depth=3):
        """Get all entries in this catalog and sub-catalogs
        Parameters
        ----------
        sofar: dict or None
            Within recursion, use this dict for output
        prefix: list of str or None
            Names of levels already visited
        depth: int
            Number of levels to descend; needed to truncate circular references
            and for cleaner output
        Returns
        -------
        Dict where the keys are the entry names in dotted syntax, and the
        values are entry instances.
        """
        out = sofar if sofar is not None else {}
        prefix = [] if prefix is None else prefix
        for name, item in self._entries.items():
            if item.container == "catalog" and depth > 1:
                # recurse with default open parameters
                try:
                    item().walk(out, prefix + [name], depth - 1)
                except Exception as e:
                    print(e)
                    pass  # ignore inability to descend
            n = ".".join(prefix + [name])
            out[n] = item
        return out                                    
            
class Five38SubCatalog(Catalog):
    name = "fivethirtyeight_sources"
    project_name = ""
    
    def __init__(self, tree = {}, **kwargs):
        self._tree = tree
        super().__init__(**kwargs)

    def search(self, text : str = None, **_) -> "Five38SubCatalog":
        return Five38SubCatalog()

    def _load(self):
        self._entries = {}
        
        if self._tree == {}:
            # Get the default branch
            response = requests.get(f"{GH_API}/repos/{REPO}")
            response.raise_for_status()

            repo = response.json()

            # Get the sha for the default branch
            response = requests.get(f"{GH_API}/repos/{REPO}/branches/{repo['default_branch']}")
            response.raise_for_status()

            branch = response.json()

            # get the full tree
            response = requests.get(f"{GH_API}/repos/{REPO}/git/trees/{branch['commit']['sha']}?recursive=1")
            response.raise_for_status()

            self._tree = response.json()
        
        sha = self._tree['sha']

        # Then add each csv as a data source to the data set Catalog
        """most projects only have one data file, but some have multiple, so loop through any csvs"""
        output_list = [x for x in self._tree['tree'] if (x['type'] == 'blob' and x['path'].startswith(f"{self.name}/") and x['path'].lower().endswith('.csv'))]

        for a_csv in output_list:
            dataset_name = a_csv['path'].replace(f"{self.name}/","").replace('-','_')
            self._entries[dataset_name] = LocalCatalogEntry(
                name=dataset_name,
                description=f"data file for {self.name}",
                driver='csv',
                catalog=self,
                args={
                    "urlpath": f"{RAW_URL}/{REPO}/{sha}/{a_csv['path']}"
                }
            )
            