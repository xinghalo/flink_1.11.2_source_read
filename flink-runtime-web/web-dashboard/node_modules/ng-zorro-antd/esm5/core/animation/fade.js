/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { animate, style, transition, trigger } from '@angular/animations';
import { AnimationDuration } from './animation';
/** @type {?} */
export var fadeMotion = trigger('fadeMotion', [
    transition(':enter', [
        style({ opacity: 0 }),
        animate("" + AnimationDuration.BASE, style({ opacity: 1 }))
    ]),
    transition(':leave', [
        style({ opacity: 1 }),
        animate("" + AnimationDuration.BASE, style({ opacity: 0 }))
    ])
]);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZmFkZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL2FuaW1hdGlvbi9mYWRlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsT0FBTyxFQUNQLEtBQUssRUFDTCxVQUFVLEVBQ1YsT0FBTyxFQUVSLE1BQU0scUJBQXFCLENBQUM7QUFDN0IsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sYUFBYSxDQUFDOztBQUVoRCxNQUFNLEtBQU8sVUFBVSxHQUE2QixPQUFPLENBQUMsWUFBWSxFQUFFO0lBQ3hFLFVBQVUsQ0FBQyxRQUFRLEVBQUU7UUFDbkIsS0FBSyxDQUFDLEVBQUUsT0FBTyxFQUFFLENBQUMsRUFBRSxDQUFDO1FBQ3JCLE9BQU8sQ0FBQyxLQUFHLGlCQUFpQixDQUFDLElBQU0sRUFBRSxLQUFLLENBQUMsRUFBRSxPQUFPLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQztLQUM1RCxDQUFDO0lBQ0YsVUFBVSxDQUFDLFFBQVEsRUFBRTtRQUNuQixLQUFLLENBQUMsRUFBRSxPQUFPLEVBQUUsQ0FBQyxFQUFFLENBQUM7UUFDckIsT0FBTyxDQUFDLEtBQUcsaUJBQWlCLENBQUMsSUFBTSxFQUFFLEtBQUssQ0FBQyxFQUFFLE9BQU8sRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDO0tBQzVELENBQUM7Q0FDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgYW5pbWF0ZSxcbiAgc3R5bGUsXG4gIHRyYW5zaXRpb24sXG4gIHRyaWdnZXIsXG4gIEFuaW1hdGlvblRyaWdnZXJNZXRhZGF0YVxufSBmcm9tICdAYW5ndWxhci9hbmltYXRpb25zJztcbmltcG9ydCB7IEFuaW1hdGlvbkR1cmF0aW9uIH0gZnJvbSAnLi9hbmltYXRpb24nO1xuXG5leHBvcnQgY29uc3QgZmFkZU1vdGlvbjogQW5pbWF0aW9uVHJpZ2dlck1ldGFkYXRhID0gdHJpZ2dlcignZmFkZU1vdGlvbicsIFtcbiAgdHJhbnNpdGlvbignOmVudGVyJywgW1xuICAgIHN0eWxlKHsgb3BhY2l0eTogMCB9KSxcbiAgICBhbmltYXRlKGAke0FuaW1hdGlvbkR1cmF0aW9uLkJBU0V9YCwgc3R5bGUoeyBvcGFjaXR5OiAxIH0pKVxuICBdKSxcbiAgdHJhbnNpdGlvbignOmxlYXZlJywgW1xuICAgIHN0eWxlKHsgb3BhY2l0eTogMSB9KSxcbiAgICBhbmltYXRlKGAke0FuaW1hdGlvbkR1cmF0aW9uLkJBU0V9YCwgc3R5bGUoeyBvcGFjaXR5OiAwIH0pKVxuICBdKVxuXSk7XG4iXX0=