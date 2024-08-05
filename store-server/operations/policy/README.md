The policy class is where all the logic for where to put and get is handled. This is broken down into 2 parts: placement and transfer policies. Placement policies are used to determine where objects should be stored while transfer policies determine where objecst are read from.

Placement Policies:
Follows the interface provided in `placement_policy/base.py`. Notably, the functions `place` and `get_ttl` are important. Here are the function signatures:

```
def place(self, req: StartUploadRequest) -> List[str]

    
def get_ttl(self, src: str = None, dst: str = None, fixed_base_region: bool = False) -> int
```

`place` is the function that returns where the objects should be placed while `get_ttl` retrieves the ttl based on where the reads are coming from. `place` makes decisions on where to place objects based on the policy. This is typically decided by whether the operation is a read or write. Reads may place if the policy dictates that a copy should be stored in the local region of the read. Writes will typically always go through. `get_ttl` is simply implemented using a calculation based on the src and dst of an object. Most policies have stateless ttl calculations. So ttl is purely based on the inputs. However, special solutions may be required for stateful policies that depend on past requests or other stored data.

Transfer Policies:
Follows the interface provided in `transfer_policy/base.py`. This interface has a `get` function which makes a decision on where to read from given a list of locations. Here is the function signature:

```
def get(self, req: LocateObjectRequest, physical_locators: List[DBPhysicalObjectLocator]) -> DBPhysicalObjectLocator
```

Transfer policies tend to be simpler and make decision based on cost or speed, which come from a set of predetermined values in a csv. For example, we know what it costs and how fast a transfer from `aws:us-west-1` to `aws:us-east-1` is.

Eviction:
An important part of the model used by skystore is ttl. When ttl expires, there needs to be a way to remove object metadata from store server. This will be done by evoking the clean object route. This route handels calculating objects that are expired and send a response back to the s3-proxy detailing what objects are expired and should be removed from cloud storage solutions. Here is what the function signature looks like:

```
async def clean_object(request: CleanObjectRequest, db: Session = Depends(get_session)) -> CleanObjectResponse:
```