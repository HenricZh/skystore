import yaml
import pandas as pd
import networkx as nx
from ..model.config import Config
import os
import json
from ..utils.definitions import (
    aws_instance_throughput_limit,
    gcp_instance_throughput_limit,
    azure_instance_throughput_limit,
)


def refine_string(s):
    parts = s.split("-")[:4]
    refined = parts[0] + ":" + "-".join(parts[1:])
    return refined


def convert_hyphen_to_colon(s):
    return s.replace(":", "-", 1)


def load_config(config_path: str) -> Config:
    with open(get_full_path(config_path), "r") as f:
        config_data = yaml.safe_load(f)
        return Config(**config_data)


def get_full_path(relative_path: str):
    # Move up to the SkyStore-Simulation directory from helpers.py
    base_path = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    full_path = os.path.join(base_path, relative_path)
    return full_path


def load_profile(file_name: str):
    return pd.read_csv(f"src/profiles/{file_name}")


def make_nx_graph(
    cost_path=None,
    throughput_path=None,
    latency_path=None,
    storage_cost_path=None,
    num_vms=1,
):
    """
    Default graph with capacity constraints and cost info
    nodes: regions, edges: links
    per edge:
        throughput: max tput achievable (gbps)
        cost: $/GB
        flow: actual flow (gbps), must be < throughput, default = 0
    """
    path = os.path.dirname(os.path.abspath(__file__))
    if cost_path is None:
        cost = pd.read_csv(os.path.join(path, "profiles", "cost.csv"))
    else:
        cost = pd.read_csv(cost_path)

    if throughput_path is None:
        throughput = pd.read_csv(os.path.join(path, "profiles", "throughput.csv"))
    else:
        throughput = pd.read_csv(throughput_path)

    if latency_path is None:
        with open(os.path.join(path, "profiles", "aws_latency.json"), "r") as f:
            latency = json.load(f)
    else:
        with open(latency_path, "r") as f:
            latency = json.load(f)

    if storage_cost_path is None:
        storage = pd.read_csv(os.path.join(path, "profiles", "storage.csv"))
    else:
        storage = pd.read_csv(storage_cost_path)

    G = nx.DiGraph()
    for _, row in throughput.iterrows():
        if row["src_region"] == row["dst_region"]:
            continue
        G.add_edge(
            row["src_region"],
            row["dst_region"],
            cost=None,
            throughput=num_vms * row["throughput_sent"] / 1e9,
        )

    # just keep aws nodes and edges
    # aws_nodes = [node for node in G.nodes if node.startswith("aws")]
    # G = G.subgraph(aws_nodes).copy()

    for _, row in cost.iterrows():
        if row["src"] in G and row["dest"] in G[row["src"]]:
            G[row["src"]][row["dest"]]["cost"] = row["cost"]

    # some pairs not in the cost grid
    no_cost_pairs = []
    for edge in G.edges.data():
        src, dst = edge[0], edge[1]
        if edge[-1]["cost"] is None:
            no_cost_pairs.append((src, dst))
    print("Unable to get egress costs for: ", no_cost_pairs)

    no_storage_cost = set()
    for _, row in storage.iterrows():
        region = row["Vendor"] + ":" + row["Region"]
        if region in G:
            if row["Group"] == "storage" and (
                row["Tier"] == "General Purpose" or row["Tier"] == "Hot"
            ):
                G.nodes[region]["priceStorage"] = row["PricePerUnit"]
        else:
            no_storage_cost.add(region)
    # print("Unable to get storage cost for: ", no_storage_cost)

    # TODO: add attributes priceGet, pricePut, and priceStorage to each node
    for node in G.nodes:
        G.nodes[node]["priceGet"] = 4.4e-07
        G.nodes[node]["pricePut"] = 5.5e-06
        if "priceStorage" not in G.nodes[node]:
            G.nodes[node]["priceStorage"] = 0.023

    for node in G.nodes:
        if not G.has_edge(node, node):
            ingress_limit = (
                aws_instance_throughput_limit[1]
                if node.startswith("aws")
                else gcp_instance_throughput_limit[1]
                if node.startswith("gcp")
                else azure_instance_throughput_limit[1]
            )
            # read from local storage, temp set to 0.7 for now
            G.add_edge(
                node, node, cost=0, throughput=num_vms * ingress_limit, latency=0.70
            )

    for src, destinations in latency.items():
        for dst, latency_value in destinations.items():
            src_node = "aws:" + src
            dst_node = "aws:" + dst
            if not G.has_edge(src_node, dst_node):
                print(f"Edge {src_node} -> {dst_node} doesn't exist")
                continue  # Skip if the edge doesn't exist
            G[src_node][dst_node]["latency"] = latency_value

    # Removed all the nodes that has any in- or out-edge with attribute latency=None
    # G.remove_nodes_from([node for node in G.nodes if any([G[node][neighbor].get("latency") is None for neighbor in G.neighbors(node)])])
    G.remove_nodes_from(["aws:eu-central-2", "aws:ap-southeast-3", "aws:eu-south-2"])
    # print("Latency attributes: ", G.edges.data("latency"))
    print(f"Number of nodes: {len(G.nodes)}")
    return G
