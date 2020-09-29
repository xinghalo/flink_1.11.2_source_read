/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { animate, style, transition, trigger } from '@angular/animations';
import { AnimationCurves, AnimationDuration } from './animation';
/** @type {?} */
export const helpMotion = trigger('helpMotion', [
    transition(':enter', [
        style({
            opacity: 0,
            transform: 'translateY(-5px)'
        }),
        animate(`${AnimationDuration.SLOW} ${AnimationCurves.EASE_IN_OUT}`, style({
            opacity: 1,
            transform: 'translateY(0)'
        }))
    ]),
    transition(':leave', [
        style({
            opacity: 1,
            transform: 'translateY(0)'
        }),
        animate(`${AnimationDuration.SLOW} ${AnimationCurves.EASE_IN_OUT}`, style({
            opacity: 0,
            transform: 'translateY(-5px)'
        }))
    ])
]);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaGVscC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL2FuaW1hdGlvbi9oZWxwLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsT0FBTyxFQUNQLEtBQUssRUFDTCxVQUFVLEVBQ1YsT0FBTyxFQUVSLE1BQU0scUJBQXFCLENBQUM7QUFDN0IsT0FBTyxFQUFFLGVBQWUsRUFBRSxpQkFBaUIsRUFBRSxNQUFNLGFBQWEsQ0FBQzs7QUFFakUsTUFBTSxPQUFPLFVBQVUsR0FBNkIsT0FBTyxDQUFDLFlBQVksRUFBRTtJQUN4RSxVQUFVLENBQUMsUUFBUSxFQUFFO1FBQ25CLEtBQUssQ0FBQztZQUNKLE9BQU8sRUFBSSxDQUFDO1lBQ1osU0FBUyxFQUFFLGtCQUFrQjtTQUM5QixDQUFDO1FBQ0YsT0FBTyxDQUFDLEdBQUcsaUJBQWlCLENBQUMsSUFBSSxJQUFJLGVBQWUsQ0FBQyxXQUFXLEVBQUUsRUFBRSxLQUFLLENBQUM7WUFDeEUsT0FBTyxFQUFJLENBQUM7WUFDWixTQUFTLEVBQUUsZUFBZTtTQUMzQixDQUFDLENBQUM7S0FDSixDQUFDO0lBQ0YsVUFBVSxDQUFDLFFBQVEsRUFBRTtRQUNuQixLQUFLLENBQUM7WUFDSixPQUFPLEVBQUksQ0FBQztZQUNaLFNBQVMsRUFBRSxlQUFlO1NBQzNCLENBQUM7UUFDRixPQUFPLENBQUMsR0FBRyxpQkFBaUIsQ0FBQyxJQUFJLElBQUksZUFBZSxDQUFDLFdBQVcsRUFBRSxFQUFFLEtBQUssQ0FBQztZQUN4RSxPQUFPLEVBQUksQ0FBQztZQUNaLFNBQVMsRUFBRSxrQkFBa0I7U0FDOUIsQ0FBQyxDQUFDO0tBQ0osQ0FBQztDQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBhbmltYXRlLFxuICBzdHlsZSxcbiAgdHJhbnNpdGlvbixcbiAgdHJpZ2dlcixcbiAgQW5pbWF0aW9uVHJpZ2dlck1ldGFkYXRhXG59IGZyb20gJ0Bhbmd1bGFyL2FuaW1hdGlvbnMnO1xuaW1wb3J0IHsgQW5pbWF0aW9uQ3VydmVzLCBBbmltYXRpb25EdXJhdGlvbiB9IGZyb20gJy4vYW5pbWF0aW9uJztcblxuZXhwb3J0IGNvbnN0IGhlbHBNb3Rpb246IEFuaW1hdGlvblRyaWdnZXJNZXRhZGF0YSA9IHRyaWdnZXIoJ2hlbHBNb3Rpb24nLCBbXG4gIHRyYW5zaXRpb24oJzplbnRlcicsIFtcbiAgICBzdHlsZSh7XG4gICAgICBvcGFjaXR5ICA6IDAsXG4gICAgICB0cmFuc2Zvcm06ICd0cmFuc2xhdGVZKC01cHgpJ1xuICAgIH0pLFxuICAgIGFuaW1hdGUoYCR7QW5pbWF0aW9uRHVyYXRpb24uU0xPV30gJHtBbmltYXRpb25DdXJ2ZXMuRUFTRV9JTl9PVVR9YCwgc3R5bGUoe1xuICAgICAgb3BhY2l0eSAgOiAxLFxuICAgICAgdHJhbnNmb3JtOiAndHJhbnNsYXRlWSgwKSdcbiAgICB9KSlcbiAgXSksXG4gIHRyYW5zaXRpb24oJzpsZWF2ZScsIFtcbiAgICBzdHlsZSh7XG4gICAgICBvcGFjaXR5ICA6IDEsXG4gICAgICB0cmFuc2Zvcm06ICd0cmFuc2xhdGVZKDApJ1xuICAgIH0pLFxuICAgIGFuaW1hdGUoYCR7QW5pbWF0aW9uRHVyYXRpb24uU0xPV30gJHtBbmltYXRpb25DdXJ2ZXMuRUFTRV9JTl9PVVR9YCwgc3R5bGUoe1xuICAgICAgb3BhY2l0eSAgOiAwLFxuICAgICAgdHJhbnNmb3JtOiAndHJhbnNsYXRlWSgtNXB4KSdcbiAgICB9KSlcbiAgXSlcbl0pO1xuIl19