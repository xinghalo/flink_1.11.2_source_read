/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { animate, style, transition, trigger } from '@angular/animations';
import { AnimationCurves, AnimationDuration } from './animation';
/** @type {?} */
export var helpMotion = trigger('helpMotion', [
    transition(':enter', [
        style({
            opacity: 0,
            transform: 'translateY(-5px)'
        }),
        animate(AnimationDuration.SLOW + " " + AnimationCurves.EASE_IN_OUT, style({
            opacity: 1,
            transform: 'translateY(0)'
        }))
    ]),
    transition(':leave', [
        style({
            opacity: 1,
            transform: 'translateY(0)'
        }),
        animate(AnimationDuration.SLOW + " " + AnimationCurves.EASE_IN_OUT, style({
            opacity: 0,
            transform: 'translateY(-5px)'
        }))
    ])
]);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaGVscC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL2FuaW1hdGlvbi9oZWxwLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsT0FBTyxFQUNQLEtBQUssRUFDTCxVQUFVLEVBQ1YsT0FBTyxFQUVSLE1BQU0scUJBQXFCLENBQUM7QUFDN0IsT0FBTyxFQUFFLGVBQWUsRUFBRSxpQkFBaUIsRUFBRSxNQUFNLGFBQWEsQ0FBQzs7QUFFakUsTUFBTSxLQUFPLFVBQVUsR0FBNkIsT0FBTyxDQUFDLFlBQVksRUFBRTtJQUN4RSxVQUFVLENBQUMsUUFBUSxFQUFFO1FBQ25CLEtBQUssQ0FBQztZQUNKLE9BQU8sRUFBSSxDQUFDO1lBQ1osU0FBUyxFQUFFLGtCQUFrQjtTQUM5QixDQUFDO1FBQ0YsT0FBTyxDQUFJLGlCQUFpQixDQUFDLElBQUksU0FBSSxlQUFlLENBQUMsV0FBYSxFQUFFLEtBQUssQ0FBQztZQUN4RSxPQUFPLEVBQUksQ0FBQztZQUNaLFNBQVMsRUFBRSxlQUFlO1NBQzNCLENBQUMsQ0FBQztLQUNKLENBQUM7SUFDRixVQUFVLENBQUMsUUFBUSxFQUFFO1FBQ25CLEtBQUssQ0FBQztZQUNKLE9BQU8sRUFBSSxDQUFDO1lBQ1osU0FBUyxFQUFFLGVBQWU7U0FDM0IsQ0FBQztRQUNGLE9BQU8sQ0FBSSxpQkFBaUIsQ0FBQyxJQUFJLFNBQUksZUFBZSxDQUFDLFdBQWEsRUFBRSxLQUFLLENBQUM7WUFDeEUsT0FBTyxFQUFJLENBQUM7WUFDWixTQUFTLEVBQUUsa0JBQWtCO1NBQzlCLENBQUMsQ0FBQztLQUNKLENBQUM7Q0FDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgYW5pbWF0ZSxcbiAgc3R5bGUsXG4gIHRyYW5zaXRpb24sXG4gIHRyaWdnZXIsXG4gIEFuaW1hdGlvblRyaWdnZXJNZXRhZGF0YVxufSBmcm9tICdAYW5ndWxhci9hbmltYXRpb25zJztcbmltcG9ydCB7IEFuaW1hdGlvbkN1cnZlcywgQW5pbWF0aW9uRHVyYXRpb24gfSBmcm9tICcuL2FuaW1hdGlvbic7XG5cbmV4cG9ydCBjb25zdCBoZWxwTW90aW9uOiBBbmltYXRpb25UcmlnZ2VyTWV0YWRhdGEgPSB0cmlnZ2VyKCdoZWxwTW90aW9uJywgW1xuICB0cmFuc2l0aW9uKCc6ZW50ZXInLCBbXG4gICAgc3R5bGUoe1xuICAgICAgb3BhY2l0eSAgOiAwLFxuICAgICAgdHJhbnNmb3JtOiAndHJhbnNsYXRlWSgtNXB4KSdcbiAgICB9KSxcbiAgICBhbmltYXRlKGAke0FuaW1hdGlvbkR1cmF0aW9uLlNMT1d9ICR7QW5pbWF0aW9uQ3VydmVzLkVBU0VfSU5fT1VUfWAsIHN0eWxlKHtcbiAgICAgIG9wYWNpdHkgIDogMSxcbiAgICAgIHRyYW5zZm9ybTogJ3RyYW5zbGF0ZVkoMCknXG4gICAgfSkpXG4gIF0pLFxuICB0cmFuc2l0aW9uKCc6bGVhdmUnLCBbXG4gICAgc3R5bGUoe1xuICAgICAgb3BhY2l0eSAgOiAxLFxuICAgICAgdHJhbnNmb3JtOiAndHJhbnNsYXRlWSgwKSdcbiAgICB9KSxcbiAgICBhbmltYXRlKGAke0FuaW1hdGlvbkR1cmF0aW9uLlNMT1d9ICR7QW5pbWF0aW9uQ3VydmVzLkVBU0VfSU5fT1VUfWAsIHN0eWxlKHtcbiAgICAgIG9wYWNpdHkgIDogMCxcbiAgICAgIHRyYW5zZm9ybTogJ3RyYW5zbGF0ZVkoLTVweCknXG4gICAgfSkpXG4gIF0pXG5dKTtcbiJdfQ==