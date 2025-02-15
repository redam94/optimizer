import plotly.graph_objects as go
from typing import Annotated

Budget = Annotated[dict[str, float], "The budget for the scenario."]

def make_radar_chart(initial_budget:Budget, optimized_budget:Budget) -> go.Figure:
    """Make a radar chart comparing the initial and optimized budgets"""
    
    #categories = [key for key in initial_budget.keys() if 'total' not in key.lower()]
    opt_categories = [key for key in optimized_budget.keys() if 'total' not in key.lower()]
    initial_total_budget = sum(initial_budget.values())
    fig = go.Figure()
    
    initial_r = [initial_budget[cat]/initial_total_budget*100 for cat in opt_categories]
    optimal_r = [optimized_budget[cat]/initial_total_budget*100 for cat in opt_categories]
    fig.add_trace(go.Scatterpolar(
        r=initial_r,
        theta=opt_categories,
        fill='toself',
        name='Initial Budget',
        hovertemplate="Channel: %{theta}<br>Percentage: %{r:.1f}%<br>Budget: $%{text}",
        text=[f"{budget*initial_total_budget/100:.2f}" for budget in initial_r],
         marker={'color': color_to_hex((80, 160, 75))}
    ))
    fig.add_trace(go.Scatterpolar(
        r=optimal_r,
        theta=opt_categories,
        fill='toself',
        name='Optimized Budget',
        hovertemplate="Channel: %{theta}<br>Percentage: %{r:.1f}%<br>Budget: $%{text}",
        text=[f"{budget*initial_total_budget/100:.2f}" for budget in optimal_r],
        marker={'color': color_to_hex((230, 100, 150))},
    ))
    #fig.update_layout(hovermode='theta unified')
    
    fig.update_layout(
        title="Initial vs Optimized Budget Allocation",
        polar=dict(
            radialaxis=dict(
            visible=True,
            range=[0, 1.2*max(initial_r+optimal_r)]
            )),
        showlegend=True,
   
    )

    return fig

def color_to_hex(rgb):
    r = str(hex(rgb[0])).replace("0x", "")
    g = str(hex(rgb[1])).replace("0x", "")
    b = str(hex(rgb[2])).replace("0x", "")
    return f'#{r}{g}{b}'
