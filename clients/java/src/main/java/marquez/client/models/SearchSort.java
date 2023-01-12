/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.client.models;

/** Sort supported for {@link SearchResult}. */
public enum SearchSort {
  NAME("name"),
  UPDATE_AT("updated_at");

  final String value;

  SearchSort(String value) {
    this.value = value;
  }
}
