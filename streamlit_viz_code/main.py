import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk

from localhost_redis import (
    references_dataframe,
    affiliations_dataframe,
    get_city_aff_count,
    removeOutlier,
)


def ref_page(selected_year):
    st.markdown("# References Analysis")
    # data
    ref_df = references_dataframe(selected_year)
    ref_count = ref_df.groupby("eid").size().value_counts().sort_index()

    ref_count = removeOutlier(ref_count)

    # bar chart
    if selected_year != "all":
        st.markdown(f"#### Distribution of references within papers in {selected_year}")
    else:
        st.markdown(f"#### Distribution of references within papers from 2018 to 2023")
    st.text(
        f"The most number of references used in paper is {ref_count.idxmax()} references."
    )

    number_of_references = ref_count.index.tolist()
    paper_count = ref_count.values.tolist()
    data = pd.DataFrame(
        {
            "number_of_references": number_of_references,
            "paper_count": paper_count,
        }
    )

    # bar chart
    fig = go.Figure(
        data=[
            go.Bar(
                x=data["number_of_references"],
                y=data["paper_count"],
                marker_color="orange",
                opacity=0.7,
            )
        ]
    )
    fig.update_layout(
        title="", xaxis_title="Number of References", yaxis_title="Paper count"
    )

    st.plotly_chart(fig)

    ########################################

    if selected_year != "all":
        st.markdown(
            f"#### Distribution of referenced year for papers in {selected_year}"
        )
    else:
        st.markdown(
            "#### Distribution of referenced year for papers in from 2018 to 2023"
        )

    ref_year_count = ref_df.groupby("year").size().sort_index()

    year = ref_year_count.index.tolist()
    count_references = ref_year_count.values.tolist()

    # bar chart
    data = pd.DataFrame(
        {
            "year": year,
            "count_references": count_references,
        }
    )

    # bar chart
    fig = go.Figure(
        data=[
            go.Bar(
                x=data["year"],
                y=data["count_references"],
                marker_color="lightblue",
                opacity=0.7,
            )
        ]
    )
    fig.update_layout(title="", xaxis_title="Year", yaxis_title="Reference count")
    st.plotly_chart(fig)

    ########################################
    if selected_year != "all":
        st.markdown(
            f"#### Time Difference Between Base Paper and Its References' Publication Years ({selected_year})"
        )
        ref_year_count = ref_df.groupby("year").size()
        for idx in ref_year_count.index:
            new_idx = abs(int(selected_year) - int(idx))
            ref_year_count.rename(index={idx: str(new_idx)}, inplace=True)

        ref_year_count.index = ref_year_count.index.astype(
            int
        )  # Convert index to integers
        ref_year_count.index.name = "year diff"
        ref_year_count = ref_year_count.sort_index(ascending=True)

        year = ref_year_count.index.tolist()
        count_references = ref_year_count.values.tolist()
        # bar chart
        data = pd.DataFrame(
            {
                "year": year,
                "count_references": count_references,
            }
        )

        # bar chart
        fig = go.Figure(
            data=[
                go.Bar(
                    x=data["year"],
                    y=data["count_references"],
                    marker_color="lightgreen",
                    opacity=0.7,
                )
            ]
        )
        fig.update_layout(
            title="", xaxis_title="Year Difference", yaxis_title="Reference count"
        )
        st.plotly_chart(fig)


def aff_page(selected_year):
    st.markdown("# Affiliations Analysis")
    city_count_df = get_city_aff_count(selected_year)

    city_count_top_df = (
        city_count_df[["city", "count"]]
        .sort_values("count", ascending=False)
        .head(3)
        .reset_index(drop=True)
    )
    city_count_top_df.index += 1

    st.text("Top 3 association city")
    st.write(city_count_top_df[["city", "count"]].head(3))

    # geo map
    st.markdown("#### Association map")
    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=50.640281,
                longitude=4.666715,
                zoom=1,
                pitch=50,
            ),
            layers=[
                pdk.Layer(
                    "ColumnLayer",
                    data=city_count_df,
                    get_position="[longitude, latitude]",
                    get_elevation="count",
                    elevation_scale=200,
                    get_fill_color="[200, 30, 0, 160]",
                    radius=20000,
                    pickable=True,
                    extruded=True,
                )
            ],
        )
    )

    ########################################

    if selected_year != "all":
        st.markdown(
            f"#### Distribution of paper affiliations by number ({selected_year})"
        )
    else:
        st.markdown(
            "#### Distribution of paper affiliations by number from 2018 to 2023"
        )
    # data
    aff_df = affiliations_dataframe(selected_year)
    aff_count = aff_df.groupby("eid").size().value_counts().sort_index()

    aff_count = removeOutlier(aff_count)

    number_of_affiliations = aff_count.index.tolist()
    paper_count = aff_count.values.tolist()
    data = pd.DataFrame(
        {
            "number_of_affiliations": number_of_affiliations,
            "paper_count": paper_count,
        }
    )

    # bar chart
    fig = go.Figure(
        data=[
            go.Bar(
                x=data["number_of_affiliations"],
                y=data["paper_count"],
                marker_color="violet",
                opacity=0.7,
            )
        ]
    )
    fig.update_layout(
        title="", xaxis_title="Number of Affiliations", yaxis_title="Paper count"
    )
    st.plotly_chart(fig)


# sidebar
with st.sidebar:
    selected_year = st.selectbox(
        "Select a year", ("2018", "2019", "2020", "2021", "2022", "2023", "all")
    )

    page_selection = st.radio("Select a page", ("References page", "Affiliations page"))

# pages
if page_selection == "References page":
    ref_page(selected_year)

elif page_selection == "Affiliations page":
    aff_page(selected_year)
