/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { animate, style, transition, trigger } from '@angular/animations';
import { AnimationDuration } from './animation';
/** @type {?} */
export const fadeMotion = trigger('fadeMotion', [
    transition(':enter', [
        style({ opacity: 0 }),
        animate(`${AnimationDuration.BASE}`, style({ opacity: 1 }))
    ]),
    transition(':leave', [
        style({ opacity: 1 }),
        animate(`${AnimationDuration.BASE}`, style({ opacity: 0 }))
    ])
]);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZmFkZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL2FuaW1hdGlvbi9mYWRlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsT0FBTyxFQUNQLEtBQUssRUFDTCxVQUFVLEVBQ1YsT0FBTyxFQUVSLE1BQU0scUJBQXFCLENBQUM7QUFDN0IsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sYUFBYSxDQUFDOztBQUVoRCxNQUFNLE9BQU8sVUFBVSxHQUE2QixPQUFPLENBQUMsWUFBWSxFQUFFO0lBQ3hFLFVBQVUsQ0FBQyxRQUFRLEVBQUU7UUFDbkIsS0FBSyxDQUFDLEVBQUUsT0FBTyxFQUFFLENBQUMsRUFBRSxDQUFDO1FBQ3JCLE9BQU8sQ0FBQyxHQUFHLGlCQUFpQixDQUFDLElBQUksRUFBRSxFQUFFLEtBQUssQ0FBQyxFQUFFLE9BQU8sRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDO0tBQzVELENBQUM7SUFDRixVQUFVLENBQUMsUUFBUSxFQUFFO1FBQ25CLEtBQUssQ0FBQyxFQUFFLE9BQU8sRUFBRSxDQUFDLEVBQUUsQ0FBQztRQUNyQixPQUFPLENBQUMsR0FBRyxpQkFBaUIsQ0FBQyxJQUFJLEVBQUUsRUFBRSxLQUFLLENBQUMsRUFBRSxPQUFPLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQztLQUM1RCxDQUFDO0NBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIGFuaW1hdGUsXG4gIHN0eWxlLFxuICB0cmFuc2l0aW9uLFxuICB0cmlnZ2VyLFxuICBBbmltYXRpb25UcmlnZ2VyTWV0YWRhdGFcbn0gZnJvbSAnQGFuZ3VsYXIvYW5pbWF0aW9ucyc7XG5pbXBvcnQgeyBBbmltYXRpb25EdXJhdGlvbiB9IGZyb20gJy4vYW5pbWF0aW9uJztcblxuZXhwb3J0IGNvbnN0IGZhZGVNb3Rpb246IEFuaW1hdGlvblRyaWdnZXJNZXRhZGF0YSA9IHRyaWdnZXIoJ2ZhZGVNb3Rpb24nLCBbXG4gIHRyYW5zaXRpb24oJzplbnRlcicsIFtcbiAgICBzdHlsZSh7IG9wYWNpdHk6IDAgfSksXG4gICAgYW5pbWF0ZShgJHtBbmltYXRpb25EdXJhdGlvbi5CQVNFfWAsIHN0eWxlKHsgb3BhY2l0eTogMSB9KSlcbiAgXSksXG4gIHRyYW5zaXRpb24oJzpsZWF2ZScsIFtcbiAgICBzdHlsZSh7IG9wYWNpdHk6IDEgfSksXG4gICAgYW5pbWF0ZShgJHtBbmltYXRpb25EdXJhdGlvbi5CQVNFfWAsIHN0eWxlKHsgb3BhY2l0eTogMCB9KSlcbiAgXSlcbl0pO1xuIl19