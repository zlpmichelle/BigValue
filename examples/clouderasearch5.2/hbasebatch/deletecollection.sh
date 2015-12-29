#!/usr/bin/env bash
solrctl collection --deletedocs example320gb-collection
solrctl collection --delete example320gb-collection
solrctl instancedir --delete example320gb-collection