/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { animate, style, transition, trigger } from '@angular/animations';
import { AnimationDuration } from './animation';
/** @type {?} */
export const moveUpMotion = trigger('moveUpMotion', [
    transition('* => enter', [
        style({
            transformOrigin: '0 0',
            transform: 'translateY(-100%)',
            opacity: 0
        }),
        animate(`${AnimationDuration.BASE}`, style({
            transformOrigin: '0 0',
            transform: 'translateY(0%)',
            opacity: 1
        }))
    ]),
    transition('* => leave', [
        style({
            transformOrigin: '0 0',
            transform: 'translateY(0%)',
            opacity: 1
        }),
        animate(`${AnimationDuration.BASE}`, style({
            transformOrigin: '0 0',
            transform: 'translateY(-100%)',
            opacity: 0
        }))
    ])
]);
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW92ZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL2FuaW1hdGlvbi9tb3ZlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsT0FBTyxFQUNQLEtBQUssRUFDTCxVQUFVLEVBQ1YsT0FBTyxFQUVSLE1BQU0scUJBQXFCLENBQUM7QUFDN0IsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sYUFBYSxDQUFDOztBQUVoRCxNQUFNLE9BQU8sWUFBWSxHQUE2QixPQUFPLENBQUMsY0FBYyxFQUFFO0lBQzVFLFVBQVUsQ0FBQyxZQUFZLEVBQUU7UUFDdkIsS0FBSyxDQUFDO1lBQ0osZUFBZSxFQUFFLEtBQUs7WUFDdEIsU0FBUyxFQUFRLG1CQUFtQjtZQUNwQyxPQUFPLEVBQVUsQ0FBQztTQUNuQixDQUFDO1FBQ0YsT0FBTyxDQUFDLEdBQUcsaUJBQWlCLENBQUMsSUFBSSxFQUFFLEVBQUUsS0FBSyxDQUFDO1lBQ3pDLGVBQWUsRUFBRSxLQUFLO1lBQ3RCLFNBQVMsRUFBUSxnQkFBZ0I7WUFDakMsT0FBTyxFQUFVLENBQUM7U0FDbkIsQ0FBQyxDQUFDO0tBQ0osQ0FBQztJQUNGLFVBQVUsQ0FBQyxZQUFZLEVBQUU7UUFDdkIsS0FBSyxDQUFDO1lBQ0osZUFBZSxFQUFFLEtBQUs7WUFDdEIsU0FBUyxFQUFRLGdCQUFnQjtZQUNqQyxPQUFPLEVBQVUsQ0FBQztTQUNuQixDQUFDO1FBQ0YsT0FBTyxDQUFDLEdBQUcsaUJBQWlCLENBQUMsSUFBSSxFQUFFLEVBQUUsS0FBSyxDQUFDO1lBQ3pDLGVBQWUsRUFBRSxLQUFLO1lBQ3RCLFNBQVMsRUFBUSxtQkFBbUI7WUFDcEMsT0FBTyxFQUFVLENBQUM7U0FDbkIsQ0FBQyxDQUFDO0tBQ0osQ0FBQztDQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBhbmltYXRlLFxuICBzdHlsZSxcbiAgdHJhbnNpdGlvbixcbiAgdHJpZ2dlcixcbiAgQW5pbWF0aW9uVHJpZ2dlck1ldGFkYXRhXG59IGZyb20gJ0Bhbmd1bGFyL2FuaW1hdGlvbnMnO1xuaW1wb3J0IHsgQW5pbWF0aW9uRHVyYXRpb24gfSBmcm9tICcuL2FuaW1hdGlvbic7XG5cbmV4cG9ydCBjb25zdCBtb3ZlVXBNb3Rpb246IEFuaW1hdGlvblRyaWdnZXJNZXRhZGF0YSA9IHRyaWdnZXIoJ21vdmVVcE1vdGlvbicsIFtcbiAgdHJhbnNpdGlvbignKiA9PiBlbnRlcicsIFtcbiAgICBzdHlsZSh7XG4gICAgICB0cmFuc2Zvcm1PcmlnaW46ICcwIDAnLFxuICAgICAgdHJhbnNmb3JtICAgICAgOiAndHJhbnNsYXRlWSgtMTAwJSknLFxuICAgICAgb3BhY2l0eSAgICAgICAgOiAwXG4gICAgfSksXG4gICAgYW5pbWF0ZShgJHtBbmltYXRpb25EdXJhdGlvbi5CQVNFfWAsIHN0eWxlKHtcbiAgICAgIHRyYW5zZm9ybU9yaWdpbjogJzAgMCcsXG4gICAgICB0cmFuc2Zvcm0gICAgICA6ICd0cmFuc2xhdGVZKDAlKScsXG4gICAgICBvcGFjaXR5ICAgICAgICA6IDFcbiAgICB9KSlcbiAgXSksXG4gIHRyYW5zaXRpb24oJyogPT4gbGVhdmUnLCBbXG4gICAgc3R5bGUoe1xuICAgICAgdHJhbnNmb3JtT3JpZ2luOiAnMCAwJyxcbiAgICAgIHRyYW5zZm9ybSAgICAgIDogJ3RyYW5zbGF0ZVkoMCUpJyxcbiAgICAgIG9wYWNpdHkgICAgICAgIDogMVxuICAgIH0pLFxuICAgIGFuaW1hdGUoYCR7QW5pbWF0aW9uRHVyYXRpb24uQkFTRX1gLCBzdHlsZSh7XG4gICAgICB0cmFuc2Zvcm1PcmlnaW46ICcwIDAnLFxuICAgICAgdHJhbnNmb3JtICAgICAgOiAndHJhbnNsYXRlWSgtMTAwJSknLFxuICAgICAgb3BhY2l0eSAgICAgICAgOiAwXG4gICAgfSkpXG4gIF0pXG5dKTtcbiJdfQ==