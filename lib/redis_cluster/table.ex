defmodule RedisCluster.Table do
  @moduledoc """
  A module for formatting lists of lists/tuples into a table-like string representation.
  Row data is converted to strings, padded for alignment, and formatted with headers.
  Assumes the headers and all rows are of the same length.
  """

  @doc """
  Converts a list of rows (lists or tuples) into a string representation of a table.

  Example:

      iex> rows = [
      ...>   ["Alice", 30, "Engineer"],
      ...>   ["Bob", 25, "Designer"]
      ...> ]
      iex> headers = ["Name", "Age", "Occupation"]
      iex> RedisCluster.Table.rows_to_string(rows, headers)
      "Name  | Age | Occupation\\n----- | --- | ----------\\nAlice | 30  | Engineer  \\nBob   | 25  | Designer  "

  The above example will produce a string that looks like this:

      Name  | Age | Occupation
      ----- | --- | ----------
      Alice | 30  | Engineer  
      Bob   | 25  | Designer 
  """
  def rows_to_string(rows, headers) do
    rows
    |> rows_to_iodata(headers)
    |> IO.iodata_to_binary()
  end

  @doc """
  Like `rows_to_string/2`, but returns `iodata` instead of a binary.
  This is more efficient if you are simply logging the output or writing it to a file.
  """
  def rows_to_iodata(rows, headers) do
    rows
    |> rows_to_columns()
    |> prepend_headers(headers)
    |> format_columns()
    |> build_rows()
    |> Enum.intersperse("\n")
  end

  ## Helpers

  defp rows_to_columns(rows) do
    rows
    |> Enum.map(&convert_row/1)
    |> Enum.zip_with(& &1)
  end

  defp convert_row(row) when is_list(row) do
    Enum.map(row, &to_string/1)
  end

  defp convert_row(row) when is_tuple(row) do
    row
    |> Tuple.to_list()
    |> convert_row()
  end

  defp prepend_headers(columns, headers) do
    Enum.zip_with(headers, columns, &[&1 | &2])
  end

  defp format_columns(columns, result \\ [])

  defp format_columns(
         [column = [header | data] | other_columns],
         result
       ) do
    width = column |> Enum.map(&String.length/1) |> Enum.max()

    new_column = [
      String.pad_trailing(header, width),
      String.duplicate("-", width)
      | Enum.map(data, &String.pad_trailing(&1, width))
    ]

    new_result = [new_column | result]

    format_columns(other_columns, new_result)
  end

  defp format_columns(_, result) do
    result
  end

  defp build_rows([]) do
    []
  end

  defp build_rows(columns) do
    Enum.reduce(columns, fn x, acc ->
      for {first, rest} <- Enum.zip(x, acc) do
        [first, " | ", rest]
      end
    end)
  end
end
