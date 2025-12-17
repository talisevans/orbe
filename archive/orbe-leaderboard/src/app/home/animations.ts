import { animate, state, style, transition, trigger } from "@angular/animations";

// this is the main pulse progress animation
export const pulseProgress = trigger( 'progressBar', [
  state('in', style({ 'flex': '{{ flex_val }}' }),
  { params: { flex_val: '*' } }
  ),
  state('out', style({ 'flex': '0 0 0%'})
  ),
  transition('* <=> *',animate('{{ time }}'))
])
