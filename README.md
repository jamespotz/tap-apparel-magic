# tap-apparel-magic

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [ApparellMagic](https://apparelmagic.com)
- Extracts the following resources:
  - Customers
  - Purchase Orders
  - Orders
  - Products
  - Vendors
  - Inventory
  - Credit Memos
  - Shipments
  - Product Attributes
  - Size Ranges
  - Payments
  - Order Items
  - Return Authorizations
  - Salespeople
  - Locations
  - Receivers
  - Currencies
  - Projects
  - Sku Warehouse
  - Warehouses
  - Terms
  - Users
  - Events
  - Pick Tickets
  - Chart Of Accounts
  - Shipping Terms
  - Ship Methods
  - Credit Memos
  - Divisions
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Quick Start 

1. Install

```sh
  pip install tap-apparel-magic
```

2. Create a config file called `config.json`. It should contain the following

```json
{
  "url": "https://example.app.apparelmagic.com/api/json",
  "token": "1234567890abcdefghijklmnop",
  "start_date": "2000-01-01T00:00:00Z"
}
```

- The `url` is your ApparellMagic shop json endpoint.

- The `token` is your ApparellMagic shop token.

- The `start date` will determine how far back in your resource history the tap will go
  - this is only relevant for the initial run, progress afterwards will be bookmarked

3. Run the Tap in Discovery mode

```sh
  tap-apparel-magic --config config.json --discover
```

- See the Singer docs on discovery mode [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode
```sh
  tap-apparel-magic -c config.json --catalog catalog-file.json
```

---

Copyright &copy; 2020 Stitch
