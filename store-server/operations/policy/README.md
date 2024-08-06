The policy class is where all the logic for where to put and get is handled. This is broken down into 2 parts: placement and transfer policies. Placement policies are used to determine where objects should be stored while transfer policies determine where objecst are read from.

### Placement Policies:
Follows the interface provided in `placement_policy/base.py`. Notably, the functions `place` and `get_ttl` are important. Here are the function signatures:

```
def place(self, req: StartUploadRequest) -> List[str]

    
def get_ttl(self, src: str = None, dst: str = None, fixed_base_region: bool = False) -> int
```

`place` is the function that returns where the objects should be placed while `get_ttl` retrieves the ttl based on where the reads are coming from. `place` makes decisions on where to place objects based on the policy. This is typically decided by whether the operation is a read or write. Reads may place if the policy dictates that a copy should be stored in the local region of the read. Writes will typically always go through. `get_ttl` is simply implemented using a calculation based on the src and dst of an object. Most policies have stateless ttl calculations. So ttl is purely based on the inputs. However, special solutions may be required for stateful policies that depend on past requests or other stored data.

### Transfer Policies:
Follows the interface provided in `transfer_policy/base.py`. This interface has a `get` function which makes a decision on where to read from given a list of locations. Here is the function signature:

```
def get(self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]) -> DBPhysicalObjectLocator
```

Transfer policies tend to be simpler and make decision based on cost or speed, which come from a set of predetermined values in a csv. For example, we know what it costs and how fast a transfer from `aws:us-west-1` to `aws:us-east-1` is.

# Eviction:
An important part of the model used by skystore is ttl. When ttl expires, there needs to be a way to remove object metadata from store server. This will be done by evoking the clean object route. This route handels calculating objects that are expired and send a response back to the s3-proxy detailing what objects are expired and should be removed from cloud storage solutions. Here is what the function signature looks like:

```
async def clean_object(request: CleanObjectRequest, db: Session = Depends(get_session)) -> CleanObjectResponse:
```

### The Place Class in Context of the entire app
In general the flow for getting data is the following:
```
proxy get -> store-server returns locations -> proxy gets data from location
```
On a get, `get_ttl` is required to determine a refresh of an object's ttl. So the function is called to determine how much the ttl should be refreshed. The ttl may change depending on the policy. The one thing that remains constant is that `storage_start_time` must be reset to represent a "refresh" of the objects ttl. This is done so the ttl doesn't have to account for the time an object is already stored for on a refresh.

In general the flow for putting data is the following:
```
proxy wants to put -> store-server returns put locations -> proxy put data in location -> notify store-server of completion
```
On a put, `get_ttl` is required to refresh the ttl. When the store-server initially returns the put locations, it utilizes get_ttl to compute what the ttl should be. Depending on the policy, this will be calculated in different ways and is abstracted away by the get_ttl function. The ttl is set in the logical object but we only start counting ttl once the put has been complete. So once the store server is notified about the completion of a put by the proxy (the last step), it will set the `storage_start_time` to the current timestamp. This ensures we start counting how long an object is stored only after it is actually inputted into a cloud storage solution. 

### Current Placemement Policies
The base region object will always have a ttl=-1 to make sure it does not get evicted when the eviction route is invoked

# Always Evict
This policy always return 0 for the ttl. This will set the ttl to 0 for all objects that are not the the base region object. 

# Always Store
This policy will set the ttl to -1. By doing so, objects will never get evicted (equivalent to always storing).

# Fixed TTL
This policy will set the ttl to a fixed number (12 hrs, 24 hrs, etc.). Objects can be evicted after ttl expires

# Push On Write
TTL is always -1 to make sure object is permanently stored on regions that are "pushed" to.

# Replicate All
TTL is always -1 to make sure object is permanently stored. This will make sure there is an object in ALL regions.

# Single Region
TTL is always -1 to make sure object is always in the region specified by the config.

# Teven
Based on the regions that have the current object and where the read is coming from, Teven is computed everytime a get/put happens. Let x be all the regions with a given object. The ttl=min(network from region x to B)/storage of object in region B if region B is where the Get/put originates.